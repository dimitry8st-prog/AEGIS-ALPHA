export async function* readMarketStream(url, options) {
  const response = await fetch(url, { ...options, cache: "no-store", redirect: "error" });
  if (!response.ok) {
    const body = await response.json().catch(() => ({}));
    throw Object.assign(new Error("Live market request failed"), {
      status: response.status, code: body.error?.code,
    });
  }
  const reader = response.body.pipeThrough(new TextDecoderStream()).getReader();
  let remainder = "";
  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) return;
      const lines = (remainder + value).split("\n");
      remainder = lines.pop();
      for (const line of lines) {
        if (line) yield JSON.parse(line);
      }
    }
  } finally {
    await reader.cancel().catch(() => {});
    reader.releaseLock();
  }
}
