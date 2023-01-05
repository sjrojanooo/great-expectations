# Great Expectations
It's a data quality tool that is used to validate, document and profile your data. The name of tool is the fundamental component, an expectation is a an assertion about your data. Very similar to a unit test, mock data is created and an assertion is tested against it. If we have a false response, we are alerted by the testing tool. The importance of unit test promote cleaner and efficient code, the only downside is that the mock data inside that unit test will never change, we leave it alone once it passes. What if our data changes at any given moment? This is where our "why" comes in. 

#### Components
The intro to the GE documentation provides us with 5 key components expectations, validation, documentation, profiling, and checkpoints. 
1. Expectations already touched on briefly above. - (Suite, Store) 
  * Wouldn't really be a great tool if we could only had one expectation for our data, instead we apply a suite of them known as a suite. 
  * These are later put into a store, that are examided by profilers and checkpoints. 
2. Validation

3. Document

4. Profiling 

5. Checkpoint
