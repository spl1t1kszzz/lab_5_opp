#include <mpi.h>
#include <pthread.h>
#include <iostream>
#include <cmath>

constexpr int tasks_count = 5000;
constexpr int tasks_lists_count = 5;
constexpr int i_need_tasks = 777;
constexpr int no_tasks_for_you = -1;
constexpr int sending_tasks_for_you = 0;
constexpr int sending_tasks_count_for_you = 1;
constexpr int executor_is_done = 2;
constexpr int alpha = 1000;

typedef struct {
    int repeat_num;
} Task;

typedef struct {
    Task *tasks;
} TaskList;

int rank, size;

int iteration_counter = 0;

double global_res = 0;

int tasks_done_counter = 0;

int tasks_left;

bool tasks_done;


// executor and handler
pthread_t threads[1];

pthread_mutex_t mutex;


TaskList tl;

void generate_tasks() {
    for (int i = 0; i < tasks_count / size; ++i) {
        tl.tasks[i].repeat_num = ((rank+1) * tasks_count / size + i) * 10;
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
            global_res += cos(0.001223);
            pthread_mutex_unlock(&mutex);
        }
        tasks_done_counter++;
    }
    tasks_left = 0;
}


void *executor_start_routine(void *args) {
    tl.tasks = new Task[tasks_count / size];
    for (int i = 0; i < tasks_lists_count; ++i) {
        MPI_Barrier(MPI_COMM_WORLD);
        tasks_left = tasks_count / size;
        generate_tasks();
        executor_job();
        pthread_mutex_lock(&mutex);
        iteration_counter++;
        pthread_mutex_unlock(&mutex);
        std::cout << "Process " << rank << " done " << tasks_count / size << " tasks." << std::endl;
        MPI_Barrier(MPI_COMM_WORLD);
    }
    delete[] tl.tasks;
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

    auto start = MPI_Wtime();
    pthread_create(&threads[0], &pthread_attr, executor_start_routine, nullptr);
    pthread_join(threads[0], nullptr);
    pthread_attr_destroy(&pthread_attr);
    pthread_mutex_destroy(&mutex);
    auto time_taken_in_proc = MPI_Wtime() - start;
    double global_time_taken;
    MPI_Reduce(&time_taken_in_proc, &global_time_taken, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
    if (rank == 0) {
        std::cout << "Time taken: " << global_time_taken << std::endl;
        std::cout << "Result: " << global_res << std::endl;
    }
    MPI_Finalize();
    return 0;
}



