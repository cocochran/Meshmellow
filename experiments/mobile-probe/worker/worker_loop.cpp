#include <chrono>
#include <csignal>
#include <thread>

int mesh_socket_main(int, char **);

int main(int argc, char ** argv) {
    std::signal(SIGPIPE, SIG_IGN);
    for (;;) {
        const int result = mesh_socket_main(argc, argv);
        if (result == 2) return result;
        if (result != 0) std::this_thread::sleep_for(std::chrono::seconds(1));
    }
}
