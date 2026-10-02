import fs from "node:fs";
import path from "node:path";
import test from "node:test";
import assert from "node:assert/strict";

import { makeTempDir } from "./helpers.mjs";
import { listJobs, readJobFile, resolveJobFile, upsertJob, writeJobFile } from "../plugins/codex/scripts/lib/state.mjs";
import { readEffectiveStoredJob, runTrackedJob } from "../plugins/codex/scripts/lib/tracked-jobs.mjs";

for (const admitted of [false, true]) {
  test(`runTrackedJob recovers an unindexed mutable gate record (admitted=${admitted})`, async () => {
    const workspaceRoot = makeTempDir();
    const job = { id: "gate-unindexed", workspaceRoot, gateKey: "response-1", status: "queued" };
    const jobFile = writeJobFile(workspaceRoot, job.id, { ...job, status: "running", pid: 2147483647 });
    fs.writeFileSync(jobFile.replace(/\.json$/, ".started.json"), JSON.stringify({
      status: "running", pid: 2147483647, startedAt: "2026-08-19T12:00:00.000Z"
    }));
    if (admitted) {
      fs.writeFileSync(jobFile.replace(/\.json$/, ".admission.json"), JSON.stringify({ status: "admitted" }));
    }
    const runner = async () => assert.fail("A dead gate must never run again");
    const result = await runTrackedJob(job, runner);
    assert.equal(result.status, "failed");
    assert.equal(result.pid, null);
    assert.deepEqual(readJobFile(jobFile), result);
    assert.equal(listJobs(workspaceRoot)[0].status, "failed");
    assert.deepEqual(await runTrackedJob(job, runner), result);
  });
}

test("lifecycle claims become visible only as complete JSON and remove their temporary files", async () => {
  const workspaceRoot = makeTempDir();
  const job = { id: "task-atomic", workspaceRoot, status: "queued" };
  const jobFile = writeJobFile(workspaceRoot, job.id, job);
  upsertJob(workspaceRoot, job);
  const originalLink = fs.linkSync;
  const published = [];
  fs.linkSync = (source, target, ...rest) => {
    if (/\.(started|admission|terminal)\.json$/.test(target)) {
      assert.equal(fs.existsSync(target), false);
      const payload = JSON.parse(fs.readFileSync(source, "utf8"));
      originalLink(source, target, ...rest);
      assert.deepEqual(JSON.parse(fs.readFileSync(target, "utf8")), payload);
      published.push(payload.status);
      return;
    }
    return originalLink(source, target, ...rest);
  };
  try {
    const result = await runTrackedJob(job, async () => ({ exitStatus: 0 }));
    assert.equal(result.exitStatus, 0);
    assert.equal(readEffectiveStoredJob(workspaceRoot, job.id).status, "completed");
  } finally {
    fs.linkSync = originalLink;
  }
  assert.deepEqual(published, ["running", "admitted", "completed"]);
  assert.equal(fs.readdirSync(path.dirname(jobFile)).some((file) => file.endsWith(".tmp")), false);
});

test("cancellation during admission publication fences the worker before it runs", async () => {
  const workspaceRoot = makeTempDir();
  const job = { id: "task-cancel-publication", workspaceRoot, status: "queued" };
  const jobFile = writeJobFile(workspaceRoot, job.id, job);
  const admissionFile = jobFile.replace(/\.json$/, ".admission.json");
  upsertJob(workspaceRoot, job);
  const originalWrite = fs.writeFileSync;
  let cancelled = false;
  fs.writeFileSync = (file, content, ...rest) => {
    const result = originalWrite(file, content, ...rest);
    if (!cancelled && String(file).startsWith(`${admissionFile}.`) && JSON.parse(content).status === "admitted") {
      cancelled = true;
      assert.equal(fs.existsSync(admissionFile), false);
      // A concurrent cancellation publishes its immutable outcome before it
      // waits for the state lock held by admission to index the mutable result.
      originalWrite(admissionFile, JSON.stringify({ status: "cancelled", completedAt: "2026-08-19T12:00:00.000Z" }), { flag: "wx" });
    }
    return result;
  };
  try {
    const result = await runTrackedJob(job, async () => assert.fail("Cancellation must prevent runner invocation"));
    assert.equal(cancelled, true);
    assert.equal(result.status, "cancelled");
    assert.equal(readEffectiveStoredJob(workspaceRoot, job.id).status, "cancelled");
  } finally {
    fs.writeFileSync = originalWrite;
  }
  assert.equal(fs.readdirSync(path.dirname(jobFile)).some((file) => file.endsWith(".tmp")), false);
});

test("runTrackedJob persists and reuses a failed admission after a stop-gate worker crashes", async () => {
  const workspaceRoot = makeTempDir();
  const job = { id: "gate-crashed", workspaceRoot, gateKey: "response-1", status: "queued" };
  const jobFile = resolveJobFile(workspaceRoot, job.id);
  fs.mkdirSync(path.dirname(jobFile), { recursive: true });
  fs.writeFileSync(jobFile.replace(/\.json$/, ".started.json"), JSON.stringify({
    status: "running", pid: 2147483647, startedAt: "2026-08-19T12:00:00.000Z"
  }));
  const runner = async () => assert.fail("A failed admission must never run");

  const result = await runTrackedJob(job, runner);

  assert.equal(result.status, "failed");
  assert.equal(result.pid, null);
  assert.match(result.errorMessage, /exited before publishing its job record/);
  assert.deepEqual(readJobFile(jobFile), result);
  const indexedJobs = listJobs(workspaceRoot);
  assert.equal(indexedJobs.length, 1);
  assert.deepEqual(indexedJobs[0], { ...result, createdAt: indexedJobs[0].createdAt, updatedAt: indexedJobs[0].updatedAt });
  assert.deepEqual(await runTrackedJob(job, runner), result);
  assert.deepEqual(readJobFile(jobFile), result);
  assert.deepEqual(listJobs(workspaceRoot), indexedJobs);
});

test("runTrackedJob does not resurrect a terminal persisted job", async () => {
  const workspaceRoot = makeTempDir();
  const job = {
    id: "task-terminal",
    workspaceRoot,
    status: "queued",
    request: { prompt: "do not run" }
  };
  const terminalJob = {
    ...job,
    status: "failed",
    phase: "failed",
    errorMessage: "Background worker exited before completing the job.",
    pid: null,
    completedAt: "2026-08-19T12:00:00.000Z"
  };
  writeJobFile(workspaceRoot, job.id, terminalJob);
  upsertJob(workspaceRoot, terminalJob);

  let runnerInvoked = false;
  const result = await runTrackedJob(job, async () => {
    runnerInvoked = true;
    return { exitStatus: 0 };
  });

  assert.equal(runnerInvoked, false);
  assert.deepEqual(result, terminalJob);
  assert.deepEqual(readJobFile(resolveJobFile(workspaceRoot, job.id)), terminalJob);
});

test("runTrackedJob returns a failed record for missing background state", async () => {
  const workspaceRoot = makeTempDir();
  const job = {
    id: "task-removed",
    workspaceRoot,
    status: "queued",
    request: { prompt: "do not run" }
  };
  const jobFile = resolveJobFile(workspaceRoot, job.id);

  let runnerInvoked = false;
  const result = await runTrackedJob(job, async () => {
    runnerInvoked = true;
    return { exitStatus: 0 };
  });

  assert.equal(runnerInvoked, false);
  assert.equal(result.status, "failed");
  assert.match(result.errorMessage, /record is missing/i);
  assert.equal(fs.existsSync(jobFile), false);
});

test("runTrackedJob returns a cancelled record after removal", async () => {
  const workspaceRoot = makeTempDir();
  const job = { id: "task-removed-fence", workspaceRoot, status: "queued", request: { prompt: "do not run" } };
  writeJobFile(workspaceRoot, job.id, job);
  upsertJob(workspaceRoot, job);
  fs.writeFileSync(resolveJobFile(workspaceRoot, job.id).replace(/\.json$/, ".removed"), "", "utf8");

  let runnerInvoked = false;
  const result = await runTrackedJob(job, async () => {
    runnerInvoked = true;
    return { exitStatus: 0 };
  });

  assert.equal(runnerInvoked, false);
  assert.equal(result.status, "cancelled");
  assert.equal(result.removed, true);
});
