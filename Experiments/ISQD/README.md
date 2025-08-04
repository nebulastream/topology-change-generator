# ISQD Experiment Data Set

In this folder we have two different set of experiment configurations: [Stateful](Stateful) and [Stateless](Stateless). 
Each configuration folder contains the following files:
- "*.toml": The main configuration file, describing the parameters to be used for conducting the exepriment ([more info](https://github.com/nebulastream/topology-change-simulator/blob/main/README.md))
- "fixed_topology.json": the json file to define a NebulaStream cluster.
- "source_group.json": the file defining the logical and physical source group. A physical source indicates where the source operator needs to be deployed.
- "topology_updates.json": the file defined the links to be removed and added at different point in time. The simulator tool uses this file to create topology change generator.

We varied the following parameters to generate different experiment configurations: _Number of Sources_, _Number of Mobile Sources_, _Rate of topology Changes_, and _Number of Intermediate Nodes_. 
Each of the parameter has the following effect:
- **Number of Sources**: the number of sources allows us to increase the size of deployed query. Higher the number of sources, more the number of nodes in the infrastructure, and higher will be the spread of the query deployment. 
This means that it will take loger for the query to be deployed due to a higher number of deployment instructions.
- **Number of Mobile Sources**: the number of mobile sources allows us to indicate the number of devices that can change their location each time a topology change event occurs.
This parameter allows to control the impact of topology changes on the number of queries and number of operators within a query.
Higher the nuber of mobile sources, more will be the number of operators affected by the topology changes.
- **Rate of Topology Changes**: this parameter defines the frequency at which the topology changes or the mobile sources move. 
This parameter is used to put the system under stress. 
Faster the rate of topology changes, more will be the query interruption.
- **Number of Intermediate Nodes**: the number of intermediate nodes defines the overall number of nodes involved in the deployment of a query.
This parameter, like the number of sources, can also influence the deployment effort involved.  

# Executing the experiments

To conduct the experiments using the listed configurations, we have provided in a separate repository an open source [Topology Change Simulator](http://github.com/nebulastream/topology-change-simulator) tool.
The tool takes the folder containing the configurations for one of the experiments as an input to configure NebulaStream to conduct the experiment. In particular, the following steps are performed during an experiment:   
- Setup a NebulaStream DSPE topology according to the fixed_topology.json file.
- Setup the logical and physical source groups according to the source_group.json file.
- Deploys 10 replicated query listed in the query_string parameter of the "*.toml" file.
- After the warmup time (defined in "*.toml"), the tool starts to generate topology changes according to topology_update.json. This file defines the number of topology changes and the rate at which the tool need to send the topology changes to the monitoring component of NebulaStream. 
- The tool then waits for cooldown time (defined in "*.toml") to terminate the experiment.  

More details about the simulator tool can be found on the [readme](https://github.com/nebulastream/topology-change-simulator/blob/main/README.md) file of the repository.