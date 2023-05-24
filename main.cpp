#include <mpi.h>
#include <pthread.h>
#include <iostream>
#include <cmath>

constexpr int tasks_count = 1000;
constexpr int tasks_lists_count = 100;
constexpr int i_need_tasks = 777;
constexpr int tasks_count_send_tag = 100;
constexpr int no_tasks_for_you = -1;
constexpr int sending_tasks_for_you = 0;

typedef struct {
    int repeat_num;
} Task;

typedef struct {
    Task *tasks;
} TaskList;

int rank, size;

int iteration_counter = 0;

double global_res = 0;

int tasks_left;

bool tasks_done;

// executor and handler
pthread_t threads[2];

pthread_mutex_t mutex;


TaskList tl;

void generate_tasks() {
    for (int i = 0; i < tasks_count; ++i) {
        tl.tasks[i].repeat_num = std::abs(rank - (iteration_counter % size));
    }
}


void executor_job(int tasks_to_do) {
    for (int i = 0; i < tasks_to_do; ++i) {
        pthread_mutex_lock(&mutex);
        int repeat = tl.tasks[i].repeat_num;
        pthread_mutex_unlock(&mutex);
        for (int j = 0; j < repeat; ++j) {
            pthread_mutex_lock(&mutex);
            global_res += cos(i * (1 / M_PI));
            pthread_mutex_unlock(&mutex);
        }
    }
}


void *executor_start_routine(void *args) {
    tl.tasks = new Task[tasks_count];
    for (int i = 0; i < tasks_lists_count; ++i) {
        MPI_Barrier(MPI_COMM_WORLD);
        generate_tasks();
        executor_job(tasks_count);
        for (int j = 0; j < size; ++j) {
            if (j != rank) {
                // я прошу задачки
                MPI_Send(&rank, 1, MPI_INT, j, i_need_tasks, MPI_COMM_WORLD);
                // ждём
                int answer;
                MPI_Recv(&answer, 1, MPI_INT, j, tasks_count_send_tag, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                if (answer != no_tasks_for_you) {
                    for (int k = 0; k < tasks_count; ++k) {
                        tl.tasks[k].repeat_num = 0;
                    }
                }
                int tasks_to_do;
                MPI_Recv(&tasks_to_do, 1, MPI_INT, j, sending_tasks_for_you, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                executor_job(tasks_to_do);
            }
        }
        MPI_Barrier(MPI_COMM_WORLD);
    }
    delete[] tl.tasks;
}


void *handler_start_routine(void *args) {
    MPI_Barrier(MPI_COMM_WORLD);
}

int main(int argc, char **argv) {
    int provided_MPI_thread_level;
    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided_MPI_thread_level);
    if (provided_MPI_thread_level != MPI_THREAD_MULTIPLE) {
        std::cerr << "Fatal error in MPI_Init_thread!" << std::endl;
        return 1;
    }
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    pthread_mutex_init(&mutex, nullptr);

    pthread_attr_t pthread_attr;
    pthread_attr_init(&pthread_attr);

    pthread_create(&threads[0], &pthread_attr, executor_start_routine, nullptr);
    pthread_create(&threads[1], &pthread_attr, handler_start_routine, nullptr);
    pthread_join(threads[0], nullptr);
    pthread_join(threads[1], nullptr);
    pthread_attr_destroy(&pthread_attr);
    pthread_mutex_destroy(&mutex);
    MPI_Finalize();
    return 0;
}