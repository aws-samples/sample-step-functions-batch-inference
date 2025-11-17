#!/usr/bin/env node
import * as cdk from "aws-cdk-lib"
import { BedrockKnowledgeBaseStack } from "../lib/bedrock-knowledge-base-stack"
import { SFNBatchInferenceStack } from "../lib/sfn-batch-inference-stack"

const app = new cdk.App()

// Create the Bedrock Knowledge Base stack first
const kbStack = new BedrockKnowledgeBaseStack(app, "BedrockKnowledgeBase", {})

// Create the main SFN Batch Inference stack with cross-stack references
const sfnStack = new SFNBatchInferenceStack(app, "SFNBatchInference", {
  knowledgeBase: kbStack.knowledgeBase,
  dataSource: kbStack.dataSource,
  textractOutputBucket: kbStack.textractOutputBucket,
  bedrockBatchBucket: kbStack.bedrockBatchBucket,
})
