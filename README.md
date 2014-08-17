jena-nosql
==========

jena-nosql lets you bind Apache Jena [1], one of the most popular RDF frameworks, with your favourite NoSQL database.  

Sometimes, working with semantic technologies means dealing with a huge amount of data so linear scalability, write and / or read performances, partition tolerance could be relevant factors especially at project startup, when things like architecture, products, frameworks need to be defined.

Jena is a powerful framework to work with Semantic Web and Linked Data applications: among other things, it provides an extensible architecture [2] that allows you to extend the core framework in order to add new kind of storages.  
Although it already provides a high performance native store called TBD [3], it is not distribuited so if you already have a NoSQL storage in your company or you are going to use that for a new Linked Data project, jena-nosql can provide you the required bridge between RDF and NoSQL world.

The project is logically divided in two main parts:

* A framework that allows to plug-in new NoSQL bindings;
* A set of concrete implementations of the framework (i.e. a set of concrete NoSQL bindings). 

If you want to use jena-nosql for your project, please check the Roadmap in order to see if the binding for your favourite storage has been already implemented. If so, follow the user guide and enjoy, otherwise you have two choices:

* wait for the module implementation; 
* write the binding on yourself, by extending the framework;

If you have question or something else, just let me know. Any feedback is warmly welcome!

Kind Regards,   
Gazza

[1] http://jena.apache.org  
[2] http://jena.apache.org/about_jena/architecture.html  
[3] http://jena.apache.org/documentation/tdb/index.html

