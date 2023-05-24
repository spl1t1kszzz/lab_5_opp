#include <mpi.h>
#include <pthread.h>
#include <iostream>
#include <cmath>

constexpr int tasks_count = 1000;
constexpr int tasks_lists_count = 100;
constexpr int i_need_tasks = 777;
constexpr int no_tasks_for_you = -1;
constexpr int sending_tasks_for_you = 0;
constexpr int sending_tasks_count_for_you = 1;
constexpr int executor_is_done = 2;
constexpr int alpha = 1000000;

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
        tl.tasks[i].repeat_num = std::abs(rank - (iteration_counter % size)) * alpha;
    }
}


void executor_job() {
    for (int i = 0; i < tasks_left; ++i) {
        pthread_mutex_lock(&mutex);
        int repeat = tl.tasks[i].repeat_num;
        pthread_mutex_unlock(&mutex);
        for (int j = 0; j < repeat; ++j) {
            pthread_mutex_lock(&mutex);
            // computers are so bad at division
            global_res += cos(i * (1 / M_PI));
            pthread_mutex_unlock(&mutex);
        }
    }
    tasks_left = 0;
}


void *executor_start_routine(void *args) {
    tl.tasks = new Task[tasks_count];
    for (int i = 0; i < tasks_lists_count; ++i) {
        MPI_Barrier(MPI_COMM_WORLD);
        generate_tasks();
        tasks_left = tasks_count;
        executor_job();
        for (int j = 0; j < size; ++j) {
            // просим всех остальных прислать мне задачи
            if (j != rank) {
                // я прошу задачки
                MPI_Send(&rank, 1, MPI_INT, j, i_need_tasks, MPI_COMM_WORLD);
                // ждём
                int answer;
                MPI_Recv(&answer, 1, MPI_INT, j, sending_tasks_count_for_you, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                if (answer != no_tasks_for_you) {
                    for (int k = 0; k < tasks_count; ++k) {
                        tl.tasks[k].repeat_num = 0;
                    }
                    // получаем задачки
                    std::cout << "waiting for tasks" << std::endl;
                    MPI_Recv(tl.tasks, answer, MPI_INT, j, sending_tasks_for_you, MPI_COMM_WORLD,
                             MPI_STATUS_IGNORE);
                    pthread_mutex_lock(&mutex);
                    tasks_left = answer;
                    pthread_mutex_unlock(&mutex);
                    // работаем дальше
                    executor_job();
                }
            }
            iteration_counter++;
        }
        MPI_Barrier(MPI_COMM_WORLD);
    }
    delete[] tl.tasks;
    pthread_mutex_lock(&mutex);
    tasks_done = true;
    pthread_mutex_unlock(&mutex);
    int Signal = executor_is_done;
    MPI_Send(&Signal, 1, MPI_INT, rank, i_need_tasks, MPI_COMM_WORLD);
    pthread_exit(nullptr);
}


void *handler_start_routine(void *args) {
    int executor_rank;
    int to_send;
    MPI_Status status;
    // синхронизируемся
    MPI_Barrier(MPI_COMM_WORLD);
    while (!tasks_done) {
        // смотрим кто попросил
        MPI_Recv(&executor_rank, 1, MPI_INT, MPI_ANY_SOURCE, i_need_tasks, MPI_COMM_WORLD, &status);
        pthread_mutex_lock(&mutex);
        // считаем сколько дать и отправляем
        if (tasks_left >= 2) {
            to_send = tasks_left / (size * 2);
            tasks_left = tasks_left / (size * 2);
            MPI_Send(&to_send, 1, MPI_INT, executor_rank, sending_tasks_count_for_you, MPI_COMM_WORLD);
            MPI_Send(&tl.tasks[tasks_count - to_send], to_send, MPI_INT, executor_rank, sending_tasks_for_you,
                     MPI_COMM_WORLD);
        } else {
            to_send = no_tasks_for_you;
            MPI_Send(&to_send, 1, MPI_INT, executor_rank, sending_tasks_count_for_you, MPI_COMM_WORLD);
        }
        pthread_mutex_unlock(&mutex);
    }
    pthread_exit(nullptr);
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
