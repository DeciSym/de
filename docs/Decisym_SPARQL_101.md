# SPARQL Tutorial
Written by: Bharath Selvaraj (bharath.selvaraj@decisym.ai)
Written on: 5/7/25

The goal of this tutorial is to give you the 101 crash course on SPARQL to begin writing queries against DeciSym Data Packages. The goal of this tutorial is for you to understand the basics of the way that graphs are build and how to query the nodes found in those graphs. 
 
SPARQL is a query language and a protocol for searching through Resource Description Framework (RDF). This is the univeral standard query language when it comes to querying RDF files. If you would like more information please refer to this page: https://www.w3.org/TR/2013/REC-sparql11-query-20130321/

## Sections
1. Data
2. Executing a SPARQL Query
3. Designing a SPARQL Query

After going through these section a user will be have a basic sense of how SPARQL queries are written and will hopefully begin to write SPARQL queries against their own datasets

### Data

To understand the basics of SPARQL, youll first need to understand what you are using SPARQL to query against. RDF Graphs are sets of triples stored togethers, and using SPARQL you can query for information found in that graph.

An RDF graph is a way to represent information like a web of simple facts.

Think of it like this:

    Imagine a big whiteboard where you can draw dots and connect them with arrows.

    Each dot is a thing—like a person, a book, or a place.

    Each arrow is a relationship—like “is the author of” or “lives in”.

    Every fact is a triple: one dot (the subject), an arrow (the relationship), and another dot (the object).
    For example:
    Alice → isFriendOf → Bob

Now, when you draw lots of these facts, they connect into a graph—a network of information that computers can understand and explore.

So in simple terms: An RDF graph is just a network of connected facts, drawn like dots and arrows, that describe things and how they relate to each other.

Take for example this visual graph representation of an apple:
