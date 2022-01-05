# DSBD_Bot
Discord Bot module

## Brief info
This module is the _Discord Bot Client_ which handles the interactions between the bot and the Discord user.

All the bot commands get processed by this module and additional processing is done by the other modules.
The requests are sent to the _API Gateway_ module which handles the transaction (and the rollback).

During the CSV File Upload process, this module _must respond_ to the API Gateway within a specified **time limit**, otherwise
the transaction will be canceled and the rollback job will be executed.