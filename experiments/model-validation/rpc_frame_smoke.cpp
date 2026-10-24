#include "ggml-alloc.h"
#include "ggml-backend.h"
#include "ggml-rpc.h"
#include "ggml.h"

#include <cstdio>
#include <vector>

int main(int argc, char ** argv) {
    if (argc != 2) {
        std::fprintf(stderr, "usage: rpc-frame-smoke host:port\n");
        return 2;
    }
    ggml_backend_t backend = ggml_backend_rpc_init(argv[1], 0);
    if (backend == nullptr) {
        return 1;
    }
    for (int asynchronous : {0, 1}) {
        for (int count : {1, 1024, 16384, 20000}) {
            ggml_init_params params = {
                8 * ggml_tensor_overhead() + ggml_graph_overhead_custom(8, false),
                nullptr,
                true,
            };
            ggml_context * ctx = ggml_init(params);
            GGML_ASSERT(ctx != nullptr);
            ggml_tensor * a = ggml_new_tensor_1d(ctx, GGML_TYPE_F32, count);
            ggml_tensor * b = ggml_new_tensor_1d(ctx, GGML_TYPE_F32, count);
            ggml_tensor * sum = ggml_add(ctx, a, b);
            ggml_cgraph * graph = ggml_new_graph_custom(ctx, 8, false);
            ggml_build_forward_expand(graph, sum);
            ggml_backend_buffer_t buffer = ggml_backend_alloc_ctx_tensors(ctx, backend);
            GGML_ASSERT(buffer != nullptr);

            std::vector<float> input(count), bias(count, 2.0f), output(count);
            for (int i = 0; i < count; ++i) {
                input[i] = i * 0.125f;
            }
            const size_t bytes = count * sizeof(float);
            if (asynchronous) {
                ggml_backend_tensor_set_async(backend, a, input.data(), 0, bytes);
                ggml_backend_tensor_set_async(backend, b, bias.data(), 0, bytes);
                GGML_ASSERT(ggml_backend_graph_compute_async(backend, graph) == GGML_STATUS_SUCCESS);
                ggml_backend_tensor_get_async(backend, sum, output.data(), 0, bytes);
                ggml_backend_synchronize(backend);
            } else {
                ggml_backend_tensor_set(a, input.data(), 0, bytes);
                ggml_backend_tensor_set(b, bias.data(), 0, bytes);
                GGML_ASSERT(ggml_backend_graph_compute(backend, graph) == GGML_STATUS_SUCCESS);
                ggml_backend_tensor_get(sum, output.data(), 0, bytes);
            }
            for (int i = 0; i < count; ++i) {
                if (output[i] != input[i] + bias[i]) {
                    std::fprintf(stderr, "wrong result at %d of %d\n", i, count);
                    return 1;
                }
            }
            ggml_backend_buffer_free(buffer);
            ggml_free(ctx);
            std::printf("passed: %d bytes per tensor, async=%d\n", int(bytes), asynchronous);
        }
    }
    ggml_backend_free(backend);
    return 0;
}
