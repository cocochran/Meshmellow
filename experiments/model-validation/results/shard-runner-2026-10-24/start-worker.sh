#!/bin/bash
set -euo pipefail
cd "$HOME/llama-rpc-demo"
artifact="models/Qwen2.5-32B-prefix-0-32.gguf"
if ! test -f "$artifact"; then
  test "$(stat -f %z "$artifact.part")" = 9828263360
  mv "$artifact.part" "$artifact"
fi
test "$(stat -f %z "$artifact")" = 9828263360
if test -f rpc.pid && kill -0 "$(cat rpc.pid)" 2>/dev/null; then
  kill -TERM "$(cat rpc.pid)"
fi
prompt='<|im_start|>system
You are a helpful assistant.<|im_end|>
<|im_start|>user
Explain why splitting one language model across two computers allows a larger model to run. Answer in two short sentences.<|im_end|>
<|im_start|>assistant
'
nohup python3 shard_endpoint.py --port 50171 \
  --expected-sha256 8455d69052c1fc055c9a1c1f48f20451c4fc6f1d725f7bf9f43b0a60f66646f6 \
  --source-sha256 2e5f6daea180dbc59f65a40641e94d3973b5dbaa32b3c0acf54647fa874e519e \
  -- "$PWD/shard-runtime-v3/mesh-shard-runner" "$PWD/$artifact" \
  0 32 512 99 11811160064 "$prompt" 0 \
  > shard-qwen32-endpoint.log 2>&1 < /dev/null &
echo $! > shard-endpoint.pid
echo "Shard endpoint PID $(cat shard-endpoint.pid); checksum validation precedes listening."
