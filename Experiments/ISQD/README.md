# ISQD Experiment Data Set

In this folder we have two different set of experiment configurations: [Stateful](Stateful) and [Stateless](Stateless). 
In particular, we varied the following parameters: _Number of Sources_, _Number of Mobile Sources_, _Rate of topology Changes_, and _Number of Intermediate Nodes_. 
Each of the parameter has the following effect:
- **Number of Sources**: number of sources allows us to increase the size of deprolyed query. Higher the number of sources, more the number of nodes in the infrastructure, and higher will be the spread of the query deployment. 
This means that it will take loger for the query to be deployed due to a higher number of deployment instructions.


# Executing the experiments

To conduct the experiments using the listed configurations, we have provided in a separate repository an open source [Topology Change Simulator](http://github.com/nebulastream/topology-change-simulator) tool.
The tool takes the folder containing the configurations for one of the experiments as an input to configure NebulaStream to conduct the experiment. In particular, the following steps are performed during an experiment:   
- Setup a NebulaStream DSPE topology according to the fixed_topology.json file.
- Setup the logical and physical source groups according to the source_group.json file.
- Deploy the query listed in the "*.toml" file.
- After the warmup time (defined in "*.toml"), the tool starts to generate topology changes according to topology_update.json. This file defines the number of 
- 