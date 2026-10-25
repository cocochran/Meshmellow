/** Read OpenAI-compatible SSE without losing frames split across network chunks. */
export async function readChatStream(response: Response, onText: (text: string) => void): Promise<string | null> {
  if (!response.body) throw new Error('The server returned no response stream.');
  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let pending = '';
  let data: string[] = [];
  let finishReason: string | null = null;
  function frame(): boolean {
    const payload = data.join('\n');
    data = [];
    if (!payload) return false;
    if (payload === '[DONE]') return true;
    const event = JSON.parse(payload);
    if (event.error) throw new Error(typeof event.error === 'string' ? event.error : event.error.message || 'Generation failed.');
    const choice = event.choices?.[0];
    if (typeof choice?.delta?.content === 'string') onText(choice.delta.content);
    if (typeof choice?.finish_reason === 'string') finishReason = choice.finish_reason;
    return false;
  }
  try {
    while (true) {
      const { value, done } = await reader.read();
      pending += decoder.decode(value, { stream: !done });
      let newline: number;
      while ((newline = pending.indexOf('\n')) >= 0) {
        const line = pending.slice(0, newline).replace(/\r$/, '');
        pending = pending.slice(newline + 1);
        if (line === '') {
          if (frame()) return finishReason;
        } else if (line.startsWith('data:')) {
          data.push(line.slice(5).replace(/^ /, ''));
        }
      }
      if (done) throw new Error('The connection ended before the reply finished. Try again.');
    }
  } finally {
    await reader.cancel().catch(() => {});
    reader.releaseLock();
  }
}
