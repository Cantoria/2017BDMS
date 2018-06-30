/* 2, 201728013229090, Yinghan Shen */

/**
 * @file 2_201728013229090_hw2.cc
 * @author  Yinghan Shen
 * @version 0.1
 *
 * @section LICENSE 
 * 
 * Copyright 2016 Shimin Chen (chensm@ict.ac.cn) and
 * Songjie Niu (niusongjie@ict.ac.cn)
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 * @section DESCRIPTION
 * 
 * This file implements the GraphColor algorithm using graphlite API.
 *
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <set>
#include <vector>
#include <ctime>
#include<algorithm>

#include "GraphLite.h"

#define VERTEX_CLASS_NAME(name) GraphColor##name


typedef struct GCNode{
	int color;
	int64_t v_id;
} GCNode;

typedef struct GlobalV{
	int64_t start_node;
	int color_num;
} GlobalV;



class VERTEX_CLASS_NAME(InputFormatter): public InputFormatter {
public:
	//extract vertex number from file
    int64_t getVertexNum() {
        unsigned long long n;
        sscanf(m_ptotal_vertex_line, "%lld", &n);
        m_total_vertex= n;
        return m_total_vertex;
    }
	//extract edge number from file 
    int64_t getEdgeNum() {
        unsigned long long n;
        sscanf(m_ptotal_edge_line, "%lld", &n);
        m_total_edge= n;
        return m_total_edge;
    }
    int getVertexValueSize() {
        m_n_value_size = sizeof(GCNode);
        return m_n_value_size;
    }
    int getEdgeValueSize() {
		// Edge value type dosen't need change
        m_e_value_size = sizeof(double);
        return m_e_value_size;
    }
    int getMessageValueSize() {
        m_m_value_size = sizeof(GCNode);
        return m_m_value_size;
    }
    void loadGraph() {
        unsigned long long last_vertex;
        unsigned long long from;
        unsigned long long to;
        double weight = 0;
        
        double value = 1;
        int outdegree = 0;
        
        const char *line= getEdgeLine();

        // Note: modify this if an edge weight is to be read
        //       modify the 'weight' variable

        sscanf(line, "%lld %lld", &from, &to);
        addEdge(from, to, &weight);

        last_vertex = from;
        ++outdegree;
        for (int64_t i = 1; i < m_total_edge; ++i) {
            line= getEdgeLine();

            // Note: modify this if an edge weight is to be read
            //       modify the 'weight' variable
			// if find next new from vertex, execute addVertex
			// otherwise add outdegree for this from vertex
            sscanf(line, "%lld %lld", &from, &to);
            if (last_vertex != from) {
                addVertex(last_vertex, &value, outdegree);
                last_vertex = from;
                outdegree = 1;
            } else {
                ++outdegree;
            }
            addEdge(from, to, &weight);
        }
        addVertex(last_vertex, &value, outdegree);
    }
};

class VERTEX_CLASS_NAME(OutputFormatter): public OutputFormatter {
public:
    void writeResult() {
        int64_t vid;
        GCNode value;
        char s[1024];

        for (ResultIterator r_iter; ! r_iter.done(); r_iter.next() ) {
            r_iter.getIdValue(vid, &value);
            int n = sprintf(s, "%lld: %d\n", (unsigned long long)vid, value.color);
            writeNextResLine(s, n);
        }
    }
};

// An aggregator that records a double value tom compute sum
class VERTEX_CLASS_NAME(Aggregator): public Aggregator<GlobalV> {
public:
    void init() {
        
    }
    void* getGlobal() {
        return &m_global;
    }
    void setGlobal(const void* p) {
        m_global = * (GlobalV *)p;
    }
    void* getLocal() {
        return &m_local;
    }
    void merge(const void* p) {
        
    }
    void accumulate(const void* p) {
        
    }
};

//template: Vertex value, Edge Value, Message value
class VERTEX_CLASS_NAME(): public Vertex <GCNode, double, GCNode> {
public:
    void compute(MessageIterator* pmsgs) {
		// get current node
		GCNode current_node;	
		GlobalV global_aggr = * (GlobalV *)getAggrGlobal(0);
        // step 0
		// we check Vertex id and start node id in cmd arg
        if (getSuperstep() == 0) {
			//if start node is current node
			//assign 0 to current node's color
           if(m_pme->m_v_id == global_aggr.start_node){
			   current_node.color = 0;
			   current_node.v_id = m_pme->m_v_id;
		   }
		   //if current node is not start node
		   //assign -1 to current node's color
		   else{
			   current_node.color = -1;
			   current_node.v_id = m_pme->m_v_id;		   		   
		   }
        } 
		else{
			current_node = getValue();
			//number of neighborhood(s)
			int64_t n = 0;
			//number of colored neightbood(s)
			int64_t setted_num = 0;
			//store neighborhood(s) color
			set<int> setted_color;
			//store uncolored neighborhood(s) v_id
			vector<int64_t> unsetted_node; 
			for(; ! pmsgs->done(); pmsgs->next()){
				n++;
				GCNode adjnode = pmsgs->getValue();
				if(adjnode.color != -1){
					setted_color.insert(adjnode.color);
					setted_num++;
				}
				else {
					unsetted_node.push_back(adjnode.v_id);					
				}
			}
			//if current node is colored, and its neighborhoods are colored
			// then we halt
			if(current_node.color != -1){
				if(setted_num == n){
					voteToHalt(); return;
				}			
			}
			//if current node is uncolored and its neighborhoods are colored
			//or current node's v_id is smallest among it's neigborhoods
			//then we assign current node a color
			else if(unsetted_node.size() == 0 ||(unsetted_node.size() > 0 && current_node.v_id < *min_element(unsetted_node.begin(), unsetted_node.end()))){
				srand((unsigned)time( NULL ) * current_node.v_id);
				current_node.color = rand() % global_aggr.color_num;
				while(setted_color.find(current_node.color) != setted_color.end()){
					current_node.color = rand() % global_aggr.color_num;
				}
			}
        } 
		*mutableValue() = current_node;
		sendMessageToAllNeighbors(current_node);
    }
};

class VERTEX_CLASS_NAME(Graph): public Graph {
public:
    VERTEX_CLASS_NAME(Aggregator)* aggregator;

public:
    // argv[0]: GraphColor.so
    // argv[1]: <input path>
    // argv[2]: <output path>
	// argv[3]: <v0id>
	// argv[4]: <color number>
    void init(int argc, char* argv[]) {

        setNumHosts(5);
        setHost(0, "localhost", 1411);
        setHost(1, "localhost", 1421);
        setHost(2, "localhost", 1431);
        setHost(3, "localhost", 1441);
        setHost(4, "localhost", 1451);

        if (argc < 5) {
           printf ("Usage: %s <input path> <output path> <v0 id> <num color>\n", argv[0]);
           exit(1);
        }
		GlobalV gv;
        m_pin_path = argv[1];
        m_pout_path = argv[2];
		gv.start_node = atoll(argv[3]);
		gv.color_num = atoi(argv[4]);

        aggregator = new VERTEX_CLASS_NAME(Aggregator)[1];
        regNumAggr(1);
        regAggr(0, &aggregator[0]);
		aggregator->setGlobal(&gv);
    }

    void term() {
        delete[] aggregator;
    }
};

/* STOP: do not change the code below. */
extern "C" Graph* create_graph() {
    Graph* pgraph = new VERTEX_CLASS_NAME(Graph);

    pgraph->m_pin_formatter = new VERTEX_CLASS_NAME(InputFormatter);
    pgraph->m_pout_formatter = new VERTEX_CLASS_NAME(OutputFormatter);
    pgraph->m_pver_base = new VERTEX_CLASS_NAME();

    return pgraph;
}

extern "C" void destroy_graph(Graph* pobject) {
    delete ( VERTEX_CLASS_NAME()* )(pobject->m_pver_base);
    delete ( VERTEX_CLASS_NAME(OutputFormatter)* )(pobject->m_pout_formatter);
    delete ( VERTEX_CLASS_NAME(InputFormatter)* )(pobject->m_pin_formatter);
    delete ( VERTEX_CLASS_NAME(Graph)* )pobject;
}