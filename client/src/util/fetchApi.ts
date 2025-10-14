// utils/fetchApi.ts
export interface FetchApiOptions {
  method?: string;
  body?: any;
  useJson?: boolean;
  timeoutMs?: number;
  headers?: Record<string, string>;
  authToken?: string | null;
}

export async function fetchApi(
  url: string,
  {
    method = "GET",
    body,
    useJson = true,
    timeoutMs = 10000, // 10s default timeout
    headers = {},
    authToken = null,
  }: FetchApiOptions = {}
) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);

  const reqHeaders: Record<string, string> = { ...headers };
  if (useJson) reqHeaders["Content-Type"] = "application/json";
  if (authToken) reqHeaders["Authorization"] = `Bearer ${authToken}`;

  try {
    const res = await fetch(url, {
      method,
      headers: reqHeaders,
      body: body ? (useJson ? JSON.stringify(body) : body) : undefined,
      signal: controller.signal,
    });

    clearTimeout(timeout);

    if (!res.ok) {
      const text = await res.text().catch(() => "");
      throw new Error(text || `${res.status} ${res.statusText}`);
    }

    // Decode response based on content type
    const contentType = res.headers.get("Content-Type") || "";
    if (contentType.includes("application/json")) return await res.json();
    if (contentType.includes("text/")) return await res.text();
    return await res.arrayBuffer();
  } catch (err: any) {
    clearTimeout(timeout);
    if (err.name === "AbortError") {
      throw new Error("Request timed out");
    }
    throw new Error(err.message || "Network error");
  }
}
