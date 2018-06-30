#!/bin/bash
cd example
make clean
make
cd ..
start-graphlite example/2_201728013229090_hw2.so ${GRAPHLITE_HOME}/part2-input/Color-graph0_4w ${GRAPHLITE_HOME}/out 0 5
start-graphlite example/2_201728013229090_hw2.so ${GRAPHLITE_HOME}/part2-input/Color-graph1_4w ${GRAPHLITE_HOME}/out 0 5