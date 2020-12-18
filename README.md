# Coroflow

This is a library for building asynchronous coroutine-based pipelines in Python.
Useful features include:

* pass data between tasks with queues
* Connect stages of the pipeline with fanout/fanin patterns or load-balancer patterns
* mix in blocking taks, coroflow will run it in threads
* has an apache ariflow like api for connecting tasks

