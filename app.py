from collections import deque
from threading import Lock
from uuid import uuid4
from flask import Flask, jsonify, request

app = Flask(__name__)


class DeadLetterQueue:
    def __init__(self, max_retries=3):
        self.pending = deque()
        self.jobs = {}
        self.dead_letters = {}
        self.max_retries = max_retries
        self.lock = Lock()

    def enqueue(self, payload):
        job = {"id": str(uuid4()), "status": "queued",
               "payload": payload, "attempts": 0}
        with self.lock:
            self.jobs[job["id"]] = job
            self.pending.append(job["id"])
        return dict(job)

    def claim(self):
        with self.lock:
            while self.pending:
                job = self.jobs[self.pending.popleft()]
                if job["status"] == "queued":
                    job["status"] = "processing"
                    job["attempts"] += 1
                    return dict(job)
        return None

    def complete(self, job_id, result):
        with self.lock:
            job = self.jobs.get(job_id)
            if not job:
                return None
            if job["status"] == "processing":
                job["status"] = "completed"
                job["result"] = result
            return dict(job)

    def fail(self, job_id, error):
        with self.lock:
            job = self.jobs.get(job_id)
            if not job:
                return None
            if job["status"] != "processing":
                return dict(job)
            job["error"] = error
            if job["attempts"] >= self.max_retries:
                job["status"] = "dead"
                self.dead_letters[job_id] = dict(job)
            else:
                job["status"] = "queued"
                self.pending.append(job_id)
            return dict(job)

    def get(self, job_id):
        with self.lock:
            job = self.jobs.get(job_id)
            return dict(job) if job else None

    def dead(self):
        with self.lock:
            return [dict(j) for j in self.dead_letters.values()]

    def requeue_dead(self, job_id):
        with self.lock:
            job = self.dead_letters.get(job_id)
            if not job:
                return None
            job["status"] = "queued"
            job["error"] = None
            self.dead_letters.pop(job_id, None)
            self.pending.append(job_id)
            return dict(job)

    def stats(self):
        with self.lock:
            counts = {"queued": 0, "processing": 0,
                      "completed": 0, "dead": 0}
            for job in self.jobs.values():
                counts[job["status"]] += 1
            counts["total"] = len(self.jobs)
            counts["dead_letters"] = len(self.dead_letters)
            return counts


queue = DeadLetterQueue(3)


@app.get("/health")
def health():
    return jsonify({"status": "ok", "service": "dead-letter-queue"})


@app.post("/api/jobs")
def enqueue():
    payload = request.get_json(silent=True)
    if not isinstance(payload, dict) or not payload:
        return jsonify({"error": "JSON object payload is required"}), 400
    return jsonify(queue.enqueue(payload)), 202


@app.post("/api/jobs/claim")
def claim():
    job = queue.claim()
    if job is None:
        return jsonify({"message": "no queued jobs"}), 204
    return jsonify(job)


@app.post("/api/jobs/<job_id>/complete")
def complete(job_id):
    body = request.get_json(silent=True)
    if not isinstance(body, dict) or "result" not in body:
        return jsonify({"error": "JSON body with 'result' is required"}), 400
    job = queue.complete(job_id, body["result"])
    if job is None:
        return jsonify({"error": "job not found"}), 404
    return jsonify(job)


@app.post("/api/jobs/<job_id>/fail")
def fail(job_id):
    body = request.get_json(silent=True)
    if not isinstance(body, dict) or not isinstance(body.get("error"), str):
        return jsonify({"error": "JSON body with string 'error' is required"}), 400
    job = queue.fail(job_id, body["error"])
    if job is None:
        return jsonify({"error": "job not found"}), 404
    return jsonify(job)


@app.get("/api/jobs/<job_id>")
def get_job(job_id):
    job = queue.get(job_id)
    if job is None:
        return jsonify({"error": "job not found"}), 404
    return jsonify(job)


@app.get("/api/dead-letters")
def dead_letters():
    return jsonify({"items": queue.dead()})


@app.post("/api/dead-letters/<job_id>/requeue")
def requeue(job_id):
    job = queue.requeue_dead(job_id)
    if job is None:
        return jsonify({"error": "dead-letter job not found"}), 404
    return jsonify(job), 202


@app.get("/api/jobs/stats")
def stats():
    return jsonify(queue.stats())


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
