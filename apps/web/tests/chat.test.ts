import test from 'node:test';
import assert from 'node:assert/strict';
import { readChatStream } from '../src/lib/chat.ts';

function response(text: string) {
  const bytes = new TextEncoder().encode(text);
  return new Response(new ReadableStream({ start(controller) {
    // Fragment UTF-8 characters, CRLF separators, and JSON across reads.
    for (let i = 0; i < bytes.length; i += 3) controller.enqueue(bytes.slice(i, i + 3));
    controller.close();
  } }));
}
test('streams fragmented UTF-8 replies and reports output-limit termination', async () => {
  let text = '';
  const reason = await readChatStream(response(': keepalive\r\n\r\ndata: {"choices":[{"delta":{"content":"Hello 🌍"}}]}\r\n\r\ndata: {"choices":[{"delta":{"content":"!"},"finish_reason":"length"}]}\r\n\r\ndata: [DONE]\r\n\r\n'), chunk => text += chunk);
  assert.equal(text, 'Hello 🌍!');
  assert.equal(reason, 'length');
});
test('surfaces a mid-stream server error', async () => {
  await assert.rejects(readChatStream(response('event: error\ndata: {"error":{"message":"Worker disconnected"}}\n\n'), () => {}), /Worker disconnected/);
});
test('does not mistake a dropped connection for a completed reply', async () => {
  await assert.rejects(readChatStream(response('data: {"choices":[{"delta":{"content":"Partial"}}]}\n\n'), () => {}), /connection ended/);
});
