# datagrids
Patterns, Utils and Libraries for working with IMDGs

## How to build
You'll need:
1. gradle installed. v2.1 at the time of writing.
2. Coherence.jar. v12.1.3 at the time of writing.
3. A environment variable called JAVA_3RD_PARTY_LIBS such that the coherence.jar can be found at JAVA_3RD_PARTY_LIBS/coherence/12.1.3.0.0/coherence.jar
4. A local copy of the code ;)

To build:
simply run 'gradle build' from the root directory

## How to use
The project is not intended to be built and used as a library. It's more just example code that can be adapted into your own code base if needed.
 
## Code areas
Here's a list of individual functional areas within the code:

### org.acc.coherence.config.example.custom.scheme
Example code for using custom scheme and cache implementations in Coherence, using the new configuration features of Coherence 12. See http://datalorax.wordpress.com/2014/10/13/using-custom-cache-implementations-in-coherence-12-2/

### org.acc.coherence.versioning
Code around versioning objects in Coherence and temporal queries. See http://datalorax.wordpress.com/2014/09/29/temporal-versioning-in-coherence/
 



