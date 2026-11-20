const el = (id) => document.getElementById(id);
const names = {
  "pdf-export": "PDF export",
  "email-send": "Email send",
  "video-transcode": "Video transcode",
};
let selected = null;
let requestKey = null;
let refreshing = false;
let previousList = "";
let previousDetail = "";
function node(tag, text, className) {
  const result = document.createElement(tag);
  if (text !== undefined) result.textContent = text;
  if (className) result.className = className;
  return result;
}
function badge(state) {
  return node("span", state, "pill " + state);
}
function time(at) {
  return new Date(at).toLocaleTimeString();
}
async function api(path, options) {
  const response = await fetch(path, { cache: "no-store", ...options });
  if (!response.ok) {
    const body = await response.json().catch(() => ({}));
    throw new Error(
      typeof body.error === "string"
        ? body.error
        : "Request failed (HTTP " + response.status + ")",
    );
  }
  return response.json();
}
function showList(rows) {
  const signature = JSON.stringify([rows, selected]);
  if (signature === previousList) return;
  previousList = signature;
  const entries = rows.map((job) => {
    const tr = node("tr");
    if (job.id === selected) tr.className = "selected";
    const button = node("button", job.id.slice(0, 8));
    button.title = "Inspect job " + job.id;
    button.onclick = () => {
      selected = job.id;
      refresh();
    };
    const id = node("td");
    id.append(button);
    const state = node("td");
    state.append(badge(job.state));
    tr.append(
      id,
      node("td", names[job.type]),
      state,
      node("td", job.attempt + " / " + job.maxAttempts),
      node("td", time(job.createdAt)),
    );
    return tr;
  });
  el("jobs").replaceChildren(...entries);
  el("empty").hidden = rows.length > 0;
}
function showDetail(job) {
  const signature = JSON.stringify(job);
  if (signature === previousDetail) return;
  previousDetail = signature;
  const heading = node("div", undefined, "job-title");
  heading.append(node("strong", names[job.type]), badge(job.state));
  const history = node("ol");
  for (const event of job.history) {
    const step = node("li");
    step.append(
      node("time", time(event.at)),
      node("span", event.state + " · Attempt " + event.attempt),
    );
    if (event.detail) step.append(node("small", event.detail));
    history.append(step);
  }
  el("detail").replaceChildren(heading, node("p", job.id, "meta"), history);
  const email = job.type === "email-send";
  const video = job.type === "video-transcode";
  const ext = video ? "mp4" : email ? "eml" : "pdf";
  const artifact = el("artifact");
  artifact.hidden = !(
    job.state === "COMPLETED" &&
    (video || email || job.type === "pdf-export") &&
    job.result?.endsWith("." + ext)
  );
  artifact.href = "/jobs/" + job.id + "/" + ext;
  artifact.textContent = video
    ? "Open MP4"
    : email
      ? "Download email"
      : "Open PDF";
  if (email && job.result?.startsWith("HTTP "))
    el("detail").append(
      node("p", "Email provider accepted the request (" + job.result + ")."),
    );
  el("retry").dataset.id = job.id;
  el("retry").hidden = job.state !== "DLQ";
}
async function refresh() {
  if (refreshing) return;
  refreshing = true;
  try {
    const rows = await api("/admin/jobs");
    if (!selected && rows.length) selected = rows[0].id;
    showList(rows);
    const id = selected;
    if (id) {
      const job = await api("/jobs/" + id);
      if (selected === id) showDetail(job);
    }
    el("connection").textContent = "Live updates";
    el("connection").className = "live";
  } catch {
    el("connection").textContent = "Connection lost · reconnecting";
    el("connection").className = "";
  } finally {
    refreshing = false;
  }
}
el("form").oninput = () => {
  requestKey = null;
  el("notice").textContent = "";
};
el("scenario").onchange = () => {
  const scenario = el("scenario").value;
  el("pdfFields").hidden = el("content").disabled = scenario !== "pdf";
  el("emailFields").hidden = el("emailFields").disabled = scenario !== "email";
  el("videoFields").hidden = el("videoFields").disabled = scenario !== "video";
  el("failureInfo").hidden = scenario !== "failure";
};
el("form").onsubmit = async (event) => {
  event.preventDefault();
  const text = el("content").value.trim();
  const pdf = el("scenario").value === "pdf";
  if (pdf && !/^[ -~]{1,200}$/.test(text)) {
    el("notice").textContent =
      "Enter one line of printable ASCII text (up to 200 characters).";
    return;
  }
  const upload = el("scenario").value === "video";
  const clip = el("videoFile").files[0];
  const types = {
    mp4: "video/mp4",
    mov: "video/quicktime",
    webm: "video/webm",
  };
  const mediaType = types[clip?.name.split(".").pop().toLowerCase()];
  if (
    upload &&
    (!clip?.size ||
      clip.size > 100 * 1024 * 1024 ||
      !Object.hasOwn(types, clip.name.split(".").pop().toLowerCase()))
  ) {
    el("notice").textContent =
      "Choose a nonempty MP4, MOV, or WebM file up to 100 MiB.";
    return;
  }
  let job;
  if (pdf) job = { type: "pdf-export", payload: { text } };
  else if (el("scenario").value === "email")
    job = {
      type: "email-send",
      payload: {
        to: el("recipient").value.trim(),
        subject: el("subject").value.trim(),
        body: el("message").value,
      },
    };
  else if (!upload)
    job = {
      type: "video-transcode",
      payload: { source: "missing-demo-input.mp4" },
    };
  requestKey ??= crypto.randomUUID();
  el("submit").disabled = true;
  try {
    if (upload) el("notice").textContent = "Uploading video…";
    const result = await api(upload ? "/jobs/video-upload" : "/jobs", {
      method: "POST",
      headers: {
        "Content-Type": upload ? mediaType : "application/json",
        "Idempotency-Key": requestKey,
      },
      body: upload ? clip : JSON.stringify(job),
    });
    selected = result.id;
    requestKey = null;
    el("notice").textContent = "Submitted job " + result.id.slice(0, 8) + ".";
    await refresh();
  } catch (error) {
    el("notice").textContent = error.message;
  } finally {
    el("submit").disabled = false;
  }
};
el("retry").onclick = async () => {
  el("retry").disabled = true;
  try {
    await api("/admin/jobs/" + el("retry").dataset.id + "/retry", {
      method: "POST",
    });
    el("notice").textContent = "Job queued for retry.";
    await refresh();
  } catch (error) {
    el("notice").textContent = error.message;
  } finally {
    el("retry").disabled = false;
  }
};
refresh();
setInterval(refresh, 1000);
