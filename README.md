# Great Expectations
It's a data quality tool that is used to validate, document and profile your data. The name of tool is the fundamental component, an expectation is a an assertion about your data. Very similar to a unit test, mock data is created and an assertion is tested against it. If we have a false response, we are alerted by the testing tool. The importance of unit test promote cleaner and efficient code, the only downside is that the mock data inside that unit test will never change, we leave it alone once it passes. What if our data changes at any given moment? This is a preemptive measure to test against the root source of our data. 

# Components
The intro to the Great Expectations documentation provides us with 5 key components expectations, validation, documentation, profiling, and checkpoints. 
1. Expectations already touched on briefly above. - (Suite, Store) 
   - Wouldn't really be a great tool if we could only had one expectation for our data, instead we apply a group of them known as a suite. 
   - These are later put into a store, and examided by profilers and checkpoints. 
2. Validation
   - Simply put this validates your data, and alerts which expectations passed or failed the suite of expectations. 
3. Document
   - This is a really awesome functionality, the suite of expectations are put inside a table of contents. You can click on each one and observe any statistics. It is a continuously updated quality report on your data. 
4. Profiling 
   - Promotes reusability of data quality checks. Instead of having to re-write a suite of expectatons you have the choice to configure or re-use a suite of expectations on from a batch of data. A new feature is the option to produced a Rule based profiler that will allow you to configure a profiler via YAML file. 
5. Checkpoint
   - Not a checkpoint for the read me... (hah..hah..) This component actually produces validation results and can also be tuned to result in an optional task to be performed. Think of it almost like a try and except, you can catch the exception and direct the next level of operations. This can be used to send an automated email, slack, or customer notification to your team members. 

# great expectations setup
1. `pip install great_expectations` will do the trick. 
2. `great_expectations init`
   - this will initialize the projects and build out the directory structure and config files. 

##### Setting up the Data Source
1. `great_expectations datasource new` 
   * Super simple we hit this command and the cli prompts to a few selections
      - Specify what data source we are connecting to 
         * For now we are working on a file system. 
      - What data processing framework are we planning to use? 
         * In this project we are going to be using pyspark
      - Enter the data path of the root directory
         * `data`
         * lucky for you I set this up just like the gxtutorial, only with different data.

2. Jupyter Notebook -> Execute / Run all cells in the notebook. 
   - PS. I did this and went to the next step in tutorial, but when I noticed my data, I realized that all of the columns were displaying as raw data without the actual columns. I decided to spend some time in the `RuntimeDataConnector`
      * This gives use different configurational commands to external data stores, which in our case is our projects file system.
      * But what does this mean? 
         - It allows us to retrieve a batch of data by defining a data path within our project. Lets retrieve the batch of data inside the data directory. 