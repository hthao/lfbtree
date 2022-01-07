
#include <gtest/gtest.h>
#include <time.h>
#include <sys/time.h>
#include <chrono>
#include <random>
#include <thread>
#include "../btree.h"
#include "../gc.h"


TEST(threads_test, insert_test)
{
    unsigned seed1 = std::chrono::system_clock::now().time_since_epoch().count();
    std::mt19937_64 g(seed1);

    int threads_count = 40;
    ebr::GCManager::instance().set_max_threads_num(40);

    int key_count = 100000;
    olcbtree::Tree<int, int> tree;

    std::thread threads[threads_count];
    for (int i = 0; i < threads_count; i++) {
        threads[i] = std::thread([&tree, &g, &key_count, &i] {

            uint64_t gc_id = ebr::GCManager::instance().register_thread();

            if (gc_id%2 == 0) {

                for (int i = 0; i < key_count; i++) {
                    tree.insert(g()%1000, i);
                }
            } else {
                for (int i = 0; i < key_count; i++) {
                    tree.remove(g()%100, gc_id);
                }
            }

            std::cout << "gc_id:" << gc_id << " gc node count: " \
                << ebr::GCManager::instance().gc_meta(gc_id).node_list_.size() \
                << std::endl;
        });
    }

    for (int i = 0; i < threads_count; i++) {
        threads[i].join();
    }

    tree.graphviz_print_tree();
}


TEST(sanity_test, insert_test)
{
    unsigned seed1 = std::chrono::system_clock::now().time_since_epoch().count();
    std::mt19937_64 g(seed1);

    int count = 100;
    olcbtree::Tree<int, int> tree;
    for (int i = 0; i < count; i++) {
        //tree.insert(g(), i);
        tree.insert(g()%10000, i);
    }

    tree.graphviz_print_tree();
}


TEST(sanity_test, asc_remove_test)
{
    unsigned seed1 = std::chrono::system_clock::now().time_since_epoch().count();
    std::mt19937_64 g(seed1);

    int count = 100000;
    olcbtree::Tree<int, int> tree;
    for (int i = 0; i < count; i++) {
        //tree.insert(g(), i);
        tree.insert(i, i);
        //tree.graphviz_print_tree();
    }

    tree.graphviz_print_tree();
    for (int i = 0; i <count ; i++) {
        //tree.remove(i);
        tree.remove(i, -1);
        tree.graphviz_print_tree();
    }
    tree.graphviz_print_tree();
}

TEST(sanity_test, desc_remove_test)
{
    unsigned seed1 = std::chrono::system_clock::now().time_since_epoch().count();
    std::mt19937_64 g(seed1);

    int count = 10;
    olcbtree::Tree<int, int> tree;
    for (int i = 0; i < count; i++) {
        //tree.insert(i, i);
        tree.insert(i, i);
        //tree.graphviz_print_tree();
    }

    tree.graphviz_print_tree();
    for (int i = count - 1; i >=0; i--) {
        //tree.remove(i);
        tree.remove(i, -1);
        tree.graphviz_print_tree();
    }
    tree.graphviz_print_tree();
}

int main(int argc, char** argv)
{
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
