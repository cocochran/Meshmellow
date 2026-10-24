#include <string>
#include <vector>

int mesh_socket_main(int, char **);

extern "C" int meshthing_native_worker(const char * model, const char * prompt, int begin, int end, int context) {
    std::vector<std::string> arguments = {"worker", "50171", model, std::to_string(begin),
        std::to_string(end), std::to_string(context), "99", "3221225472", prompt, "0"};
    std::vector<char *> pointers;
    for (auto & argument : arguments) pointers.push_back(argument.data());
    return mesh_socket_main(int(pointers.size()), pointers.data());
}
