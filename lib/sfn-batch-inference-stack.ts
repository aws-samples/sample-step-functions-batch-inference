import * as cdk from "aws-cdk-lib"
import * as bedrock from "aws-cdk-lib/aws-bedrock"
import * as dynamodb from "aws-cdk-lib/aws-dynamodb"
import * as eventBridge from "aws-cdk-lib/aws-events"
import * as eventBridgeTargets from "aws-cdk-lib/aws-events-targets"
import * as iam from "aws-cdk-lib/aws-iam"
import * as lambda from "aws-cdk-lib/aws-lambda"
import * as cloudwatch from "aws-cdk-lib/aws-logs"
import * as s3 from "aws-cdk-lib/aws-s3"
import * as sns from "aws-cdk-lib/aws-sns"
import * as sfn from "aws-cdk-lib/aws-stepfunctions"
import * as tasks from "aws-cdk-lib/aws-stepfunctions-tasks"
import { Construct } from "constructs"

const LAMBDA_PYTHON_VERSION = lambda.Runtime.PYTHON_3_13
const MODEL_ID = "us.amazon.nova-pro-v1:0"

const BEDROCK_BATCH_RESULTS_PREFIX = "batch-output"

// Add one or more emails in this list to be notified of Bedrock job transtions (ie, starting,
// completed, error) and to reveive a download link once the job has completed. Each user in this
// list will receive an email to verify the subscription.
const NOTIFICATION_EMAILS: string[] = []

export interface SFNBatchInferenceStackProps extends cdk.StackProps {
  knowledgeBase: bedrock.CfnKnowledgeBase
  dataSource: bedrock.CfnDataSource
  textractOutputBucket: s3.IBucket
  bedrockBatchBucket: s3.IBucket
}

export class SFNBatchInferenceStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props: SFNBatchInferenceStackProps) {
    super(scope, id, props)

    const { knowledgeBase, dataSource, textractOutputBucket, bedrockBatchBucket } = props

    // SNS topic that is used to send email notifications.
    const notificationTopic = this.createSNSNotificationTopic()

    // Create SNS Topic that is used when Textract jobs complete.
    const { textractRole, textractSnsTopic } = this.createSNSTopic()

    // Add CloudWatch Logs as target for EventBridge rules, which s for debugging pupposes.
    const debugLogGroup = this.createCloudWatchDebugLogGroup()

    // Create S3 buckets
    const { batchBucket, manifestFileCreatedRule } = this.createS3Buckets()

    // DDB table to store SFN task tokens.
    const dynamoTable = this.createDynamoDBTable()

    // Lambda function that will fetch the results from Textract, once the job completes.
    const textractResultsLambda = this.createTextractResultsLambda(textractOutputBucket)

    // Lambda function that will prepare the batch inference file, and write it to S3.
    const prepareBedrockLambda = this.createPrepareBedrockBatchLambda(
      textractOutputBucket,
      bedrockBatchBucket
    )

    // Lambda function that will parse the Bedrock inference results file and create metadata.json files
    const bedrockResultsExtractorLambda = this.createBedrockResultsExtractorLambda(
      textractOutputBucket,
      bedrockBatchBucket,
      notificationTopic
    )

    // Data source sync operations now use native Step Functions API calls

    // This is the role that the Step Function workflow needs in order to call the Bedrock batch
    // inference.
    const bedrockRole = this.createBedrockBatchServiceRole(bedrockBatchBucket)

    // This is the engine block of the entire process. Note that here we add an SNS publish
    // task to Textract job, so that Textract will notify us on the SNS topic once its completed.
    const stateMachine = this.createStepFunctionStateMachine(
      textractRole.roleArn,
      textractSnsTopic.topicArn,
      dynamoTable,
      textractResultsLambda,
      batchBucket,
      prepareBedrockLambda,
      bedrockRole.roleArn,
      bedrockResultsExtractorLambda,
      knowledgeBase,
      dataSource,
      notificationTopic
    )
    // allow the SFN workflow to pass the bedrock role to Bedrock.
    bedrockRole.grantPassRole(stateMachine.role)
    // Add the Step Function workflow as target whenever the manifest file is uploaded.
    manifestFileCreatedRule.addTarget(new eventBridgeTargets.SfnStateMachine(stateMachine))
    // Also add the cloudwatch group as a target, for debugging.
    manifestFileCreatedRule.addTarget(new eventBridgeTargets.CloudWatchLogGroup(debugLogGroup))

    // Lambda function that writes to DDB and sends the task token to the SFN workflow to resume
    // workflow execution.
    const textractCompleteLambda = this.createTextractCallbackLambda(dynamoTable, stateMachine)
    // Add Lambda as subscriber to SNS topic
    textractSnsTopic.addSubscription(
      new cdk.aws_sns_subscriptions.LambdaSubscription(textractCompleteLambda)
    )
    // Grant SNS permissions to call the Lambda
    textractSnsTopic.grantPublish(textractCompleteLambda)

    // Lambda function for handling Bedrock batch completion
    const bedrockCompleteLambda = this.createBedrockCallbackLambda(dynamoTable, stateMachine)

    // Create EventBridge rule for Bedrock batch completion and add Lambda target
    const bedrockBatchCompletedRule = this.createBedrockBatchCompletedRule()
    bedrockBatchCompletedRule.addTarget(
      new eventBridgeTargets.LambdaFunction(bedrockCompleteLambda)
    )

    // Create an EventBridge rule to trigger on Bedrock batch state changes for monitoring
    const bedrockStateChangeRule = this.createBedrockStateChangeRule()
    // Send to the SNS notification topic.
    bedrockStateChangeRule.addTarget(
      new eventBridgeTargets.SnsTopic(notificationTopic, {
        message: eventBridge.RuleTargetInput.fromText(
          `Batch Inference Job State Change: ${eventBridge.EventField.fromPath("$.detail.status")}`
        ),
      })
    )
    // Send the same to the cloudwatch logs group, for debugging.
    bedrockStateChangeRule.addTarget(new eventBridgeTargets.CloudWatchLogGroup(debugLogGroup))
  }

  private readonly LPTLayer = lambda.LayerVersion.fromLayerVersionArn(
    this,
    "LPTLayer",
    `arn:aws:lambda:${
      this.region
    }:017000801446:layer:AWSLambdaPowertoolsPythonV3-${LAMBDA_PYTHON_VERSION.toString().replace(
      ".",
      ""
    )}-arm64:4`
  )

  /**
   * DynamoDB table that will store textract JobIds and Step Functions task tokens.
   */
  private createDynamoDBTable = () => {
    return new dynamodb.Table(this, "TextractJobsTable", {
      partitionKey: { name: "JobId", type: dynamodb.AttributeType.STRING },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    })
  }

  /**
   * Lambda function which is called after a Textract job is complete. The job of this function is
   * to extract the Step Functions TaskToken from DynamoDB and send it to the Step Function, which
   * will resume the paused workflow.
   *
   * @param dynamoTable Table where the Textract Job id and Task Token are stored.
   * @param stateMachine The workflow to send the TaskToken
   * @returns
   */
  private createTextractCallbackLambda(
    dynamoTable: cdk.aws_dynamodb.Table,
    stateMachine: sfn.StateMachine
  ) {
    const textractCompleteLambda = new lambda.Function(this, "NotificationHandler", {
      runtime: LAMBDA_PYTHON_VERSION,
      handler: "textract_callback.handle_textract_task_complete",
      code: lambda.Code.fromAsset("src/textract-handlers"),
      layers: [this.LPTLayer],
      architecture: lambda.Architecture.ARM_64,
      environment: {
        DYNAMODB_TABLE: dynamoTable.tableName,
        POWERTOOLS_SERVICE_NAME: "textract-notifier",
        POWERTOOLS_LOG_LEVEL: "INFO",
      },
    })

    textractCompleteLambda.addToRolePolicy(
      new iam.PolicyStatement({
        actions: ["dynamodb:PutItem", "dynamodb:GetItem", "dynamodb:DeleteItem"],
        resources: [dynamoTable.tableArn],
      })
    )
    textractCompleteLambda.addToRolePolicy(
      new iam.PolicyStatement({
        actions: ["states:SendTaskSuccess"],
        resources: [stateMachine.stateMachineArn],
      })
    )

    return textractCompleteLambda
  }

  /**
   * Lambda function which is called after a Bedrock batch job is complete.
   */
  private createBedrockCallbackLambda(
    dynamoTable: cdk.aws_dynamodb.Table,
    stateMachine: sfn.StateMachine
  ) {
    const bedrockCompleteLambda = new lambda.Function(this, "BedrockCallbackHandler", {
      runtime: LAMBDA_PYTHON_VERSION,
      handler: "bedrock_callback.handle_bedrock_task_complete",
      code: lambda.Code.fromAsset("src/bedrock-handlers"),
      layers: [this.LPTLayer],
      architecture: lambda.Architecture.ARM_64,
      environment: {
        DYNAMODB_TABLE: dynamoTable.tableName,
        POWERTOOLS_SERVICE_NAME: "bedrock-notifier",
        POWERTOOLS_LOG_LEVEL: "INFO",
      },
    })

    bedrockCompleteLambda.addToRolePolicy(
      new iam.PolicyStatement({
        actions: ["dynamodb:GetItem"],
        resources: [dynamoTable.tableArn],
      })
    )
    bedrockCompleteLambda.addToRolePolicy(
      new iam.PolicyStatement({
        actions: ["states:SendTaskSuccess"],
        resources: [stateMachine.stateMachineArn],
      })
    )

    return bedrockCompleteLambda
  }

  private createTextractResultsLambda(textractOutputBucket: s3.IBucket): lambda.Function {
    const textractResultsLambda = new lambda.Function(this, "TextractResultsHandler", {
      runtime: LAMBDA_PYTHON_VERSION,
      handler: "textract_results.handle_textract_results",
      code: lambda.Code.fromAsset("src/textract-handlers"),
      layers: [this.LPTLayer],
      architecture: lambda.Architecture.ARM_64,
      timeout: cdk.Duration.minutes(1),
      memorySize: 1024,
      environment: {
        POWERTOOLS_SERVICE_NAME: "textract-results",
        POWERTOOLS_LOG_LEVEL: "INFO",
        OUTPUT_BUCKET: textractOutputBucket.bucketName,
      },
    })

    textractResultsLambda.addToRolePolicy(
      new iam.PolicyStatement({
        actions: ["textract:GetDocumentAnalysis"],
        resources: ["*"],
      })
    )
    textractResultsLambda.addToRolePolicy(
      new iam.PolicyStatement({
        actions: ["s3:PutObject"],
        resources: [textractOutputBucket.arnForObjects("*")],
      })
    )

    return textractResultsLambda
  }

  private createBedrockResultsExtractorLambda(
    textractOutputBucket: s3.IBucket,
    bedrockBatchBucket: s3.IBucket,
    notificationTopic: sns.Topic
  ): lambda.Function {
    const fn = new lambda.Function(this, "BedrockResultsExtractor", {
      runtime: LAMBDA_PYTHON_VERSION,
      handler: "handler.extract_bedrock_results",
      code: lambda.Code.fromAsset("src/bedrock-extractor"),
      layers: [this.LPTLayer],
      architecture: lambda.Architecture.ARM_64,
      timeout: cdk.Duration.minutes(5),
      memorySize: 1024,
      environment: {
        POWERTOOLS_SERVICE_NAME: "bedrock-results",
        POWERTOOLS_LOG_LEVEL: "INFO",
        INPUT_BUCKET: bedrockBatchBucket.bucketName,
        OUTPUT_BUCKET: textractOutputBucket.bucketName,
        BEDROCK_BATCH_RESULTS_PREFIX,
        SNS_NOTIFICATION_TOPIC: notificationTopic.topicArn,
      },
    })

    // Grant permissions to read from bedrock batch bucket and write to textract output bucket
    bedrockBatchBucket.grantRead(fn)
    textractOutputBucket.grantReadWrite(fn)
    notificationTopic.grantPublish(fn)

    return fn
  }

  private createStepFunctionStateMachine(
    textractRoleArn: string,
    snsTopicArn: string,
    ddbTable: dynamodb.Table,
    textractResultsLambda: lambda.Function,
    sourceBucket: s3.Bucket,
    prepareBedrockBatchLambda: cdk.aws_lambda.Function,
    bedrockRoleArn: string,
    bedrockResultsExtractorLambda: lambda.Function,
    knowledgeBase: bedrock.CfnKnowledgeBase,
    dataSource: bedrock.CfnDataSource,
    notificationTopic: sns.Topic
  ) {
    const startTextractStep = new tasks.CallAwsService(this, "StartTextractJob", {
      service: "textract",
      action: "startDocumentAnalysis",
      resultPath: "$.textractOutput",
      parameters: {
        DocumentLocation: {
          S3Object: {
            Bucket: sourceBucket.bucketName,
            Name: sfn.JsonPath.stringAt("$.filename"),
          },
        },
        FeatureTypes: ["LAYOUT"],
        NotificationChannel: {
          RoleArn: textractRoleArn,
          SnsTopicArn: snsTopicArn,
        },
      },
      iamResources: [`arn:aws:textract:${this.region}:${this.account}:*`],
    })

    const storeTextractTaskTokenStep = new tasks.CallAwsService(this, "StoreTextractTaskToken", {
      service: "dynamodb",
      integrationPattern: sfn.IntegrationPattern.WAIT_FOR_TASK_TOKEN,
      action: "putItem",
      parameters: {
        TableName: ddbTable.tableName,
        Item: {
          JobId: {
            S: sfn.JsonPath.stringAt("$.textractOutput.JobId"),
          },
          CreatedAt: {
            S: sfn.JsonPath.stateEnteredTime,
          },
          TaskToken: { S: sfn.JsonPath.taskToken },
          OriginalFile: {
            S: sfn.JsonPath.stringAt("$.filename"),
          },
        },
      },
      iamResources: ["*"],
    })

    const fetchTextractResultsStep = new tasks.LambdaInvoke(this, "FetchTextractResults", {
      lambdaFunction: textractResultsLambda,
      retryOnServiceExceptions: true,
      outputPath: "$.Payload",
    })

    const prepareBedrockBatchStep = new tasks.LambdaInvoke(this, "PrepareBedrockBatch", {
      lambdaFunction: prepareBedrockBatchLambda,
    })

    const extractJobArnStep = new sfn.Pass(this, "ExtractJobArn", {
      parameters: {
        "jobArn.$": "$.bedrockJob.JobArn",
        "payload.$": "$.Payload",
      },
    })

    const storeBedrockBatchTaskTokenStep = new tasks.CallAwsService(this, "StoreBedrockTaskToken", {
      service: "dynamodb",
      integrationPattern: sfn.IntegrationPattern.WAIT_FOR_TASK_TOKEN,
      action: "putItem",
      parameters: {
        TableName: ddbTable.tableName,
        Item: {
          JobId: {
            S: sfn.JsonPath.stringAt("$.payload.job_name"),
          },
          CreatedAt: {
            S: sfn.JsonPath.stateEnteredTime,
          },
          TaskToken: { S: sfn.JsonPath.taskToken },
          JobArn: {
            S: sfn.JsonPath.stringAt("$.jobArn"),
          },
        },
      },
      iamResources: ["*"],
      resultPath: "$.dynamoResult",
    })

    const fetchBedrockRestulsStep = new tasks.LambdaInvoke(this, "FetchBedrockResults", {
      lambdaFunction: textractResultsLambda,
      retryOnServiceExceptions: true,
      outputPath: "$.Payload",
    })

    const startBedrockBatchJob = new tasks.CallAwsService(this, "StartBedrockBatchJob", {
      service: "bedrock",
      action: "createModelInvocationJob",
      parameters: {
        InputDataConfig: {
          S3InputDataConfig: {
            S3Uri: sfn.JsonPath.stringAt("$.Payload.input_s3_uri"),
          },
        },
        OutputDataConfig: {
          S3OutputDataConfig: {
            S3Uri: sfn.JsonPath.stringAt("$.Payload.output_s3_uri"),
          },
        },
        JobName: sfn.JsonPath.stringAt("$.Payload.job_name"),
        ModelId: MODEL_ID,
        RoleArn: bedrockRoleArn,
      },
      resultPath: "$.bedrockJob",
      iamResources: ["*"],
    })

    // Extract Bedrock results and create metadata.json files
    const extractBedrockResultsStep = new tasks.LambdaInvoke(this, "ExtractBedrockResults", {
      lambdaFunction: bedrockResultsExtractorLambda,
      inputPath: "$",
      resultPath: "$.extractionResults",
    })

    // Start knowledge base data source sync using native API call
    const startIngestionJob = new tasks.CallAwsService(this, "StartIngestionJob", {
      service: "bedrockagent",
      action: "startIngestionJob",
      parameters: {
        DataSourceId: dataSource.attrDataSourceId,
        KnowledgeBaseId: knowledgeBase.attrKnowledgeBaseId,
      },
      resultPath: "$.ingestionJob",
      iamResources: ["*"],
    })

    // Wait before checking ingestion status
    const waitForIngestionProgress = new sfn.Wait(this, "WaitForIngestionProgress", {
      time: sfn.WaitTime.duration(cdk.Duration.minutes(1)),
    })

    // Get ingestion job status using native API call
    const getIngestionJob = new tasks.CallAwsService(this, "GetIngestionJob", {
      service: "bedrockagent",
      action: "getIngestionJob",
      parameters: {
        IngestionJobId: sfn.JsonPath.stringAt("$.ingestionJob.IngestionJob.IngestionJobId"),
        DataSourceId: dataSource.attrDataSourceId,
        KnowledgeBaseId: knowledgeBase.attrKnowledgeBaseId,
      },
      resultPath: "$.ingestionJobStatus",
      iamResources: ["*"],
    })

    // Send final completion notification
    const sendCompletionNotification = new tasks.SnsPublish(this, "SendCompletionNotification", {
      topic: notificationTopic,
      subject: "Batch Processing Complete - Knowledge Base Updated",
      message: sfn.TaskInput.fromJsonPathAt("$.extractionResults"),
    })

    // Choice state to check if ingestion is complete
    const isIngestionComplete = new sfn.Choice(this, "IsIngestionComplete")
      .when(
        sfn.Condition.stringEquals("$.ingestionJobStatus.IngestionJob.Status", "COMPLETE"),
        sendCompletionNotification
      )
      .when(
        sfn.Condition.stringEquals("$.ingestionJobStatus.IngestionJob.Status", "FAILED"),
        new sfn.Fail(this, "IngestionFailed", {
          cause: "Knowledge base ingestion failed",
          error: "IngestionFailure",
        })
      )
      .when(
        sfn.Condition.stringEquals("$.ingestionJobStatus.IngestionJob.Status", "STOPPED"),
        new sfn.Fail(this, "IngestionStopped", {
          cause: "Knowledge base ingestion was stopped",
          error: "IngestionStopped",
        })
      )
      .otherwise(waitForIngestionProgress)

    // Connect the ingestion wait loop
    waitForIngestionProgress.next(getIngestionJob).next(isIngestionComplete)

    const distributedMap = new sfn.DistributedMap(this, "DistributedMap", {
      mapExecutionType: sfn.StateMachineType.STANDARD,
      maxConcurrency: 10,
      itemReader: new sfn.S3JsonItemReader({
        bucket: sourceBucket,
        key: "manifest.json",
      }),
      resultPath: "$.files",
    })

    // Create the chain that will be run within the distributed map run.
    distributedMap.itemProcessor(
      startTextractStep.next(storeTextractTaskTokenStep).next(fetchTextractResultsStep)
    )

    // After the distributed map completes, prepare and start Bedrock batch job
    const mainWorkflow = distributedMap
      .next(prepareBedrockBatchStep)
      .next(startBedrockBatchJob)
      .next(extractJobArnStep)
      .next(storeBedrockBatchTaskTokenStep)
      .next(extractBedrockResultsStep)
      .next(startIngestionJob)
      .next(waitForIngestionProgress)

    // Create the state machine
    const stateMachine = new sfn.StateMachine(this, "StateMachine", {
      definitionBody: sfn.DefinitionBody.fromChainable(mainWorkflow),
      stateMachineType: sfn.StateMachineType.STANDARD,
    })

    // Grant the workflow permissions for the direct calls that it makes.
    const textractPolicy = new iam.Policy(this, "TextractPolicy", {
      statements: [
        new iam.PolicyStatement({
          actions: ["textract:GetDocumentAnalysis", "textract:StartDocumentAnalysis"],
          resources: ["*"],
        }),
      ],
    })
    stateMachine.role.attachInlinePolicy(textractPolicy)

    const bedrockPolicy = new iam.Policy(this, "BedrockInvokePolicy", {
      statements: [
        new iam.PolicyStatement({
          actions: [
            "bedrock:CreateModelInvocationJob",
            "bedrock:GetModelInvocationJob",
            "bedrock:ListModelInvocationJobs",
            "bedrock:StartIngestionJob",
            "bedrock:GetIngestionJob",
          ],
          resources: ["*"],
        }),
      ],
    })
    stateMachine.role.attachInlinePolicy(bedrockPolicy)

    // Grant S3 read permissions to Step Function.
    sourceBucket.grantRead(stateMachine)

    return stateMachine
  }

  private createSNSTopic() {
    const textractSnsTopic = new sns.Topic(this, "TextractCompletionTopic", {
      displayName: "Textract Analysis Completion Notifications",
      topicName: "textract-completion-notifications",
    })

    const textractRole = new iam.Role(this, "TextractServiceRole", {
      assumedBy: new iam.ServicePrincipal("textract.amazonaws.com"),
      description: "Role for Textract to publish to SNS topic",
    })

    textractRole.addToPolicy(
      new iam.PolicyStatement({
        effect: iam.Effect.ALLOW,
        actions: ["sns:Publish"],
        resources: [textractSnsTopic.topicArn],
      })
    )
    return { textractRole, textractSnsTopic }
  }
  /**
   * Validates if a string is a properly formatted email address
   * @param email The email address to validate
   * @returns boolean indicating if email is valid
   */
  private validateEmail(email: string): boolean {
    const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/
    return emailRegex.test(email)
  }

  private createSNSNotificationTopic() {
    // Create SNS topic for notifications
    const notificationTopic = new sns.Topic(this, "NotificationTopic", {
      displayName: "Study Review Notifications",
      topicName: "study-review-notifications",
    })

    // Simple step to validate the email addresses.
    const emails: string[] = NOTIFICATION_EMAILS.filter((email) => this.validateEmail(email)).map(
      (email) => email.trim()
    )

    emails.forEach((email) => {
      if (email) {
        notificationTopic.addSubscription(new cdk.aws_sns_subscriptions.EmailSubscription(email))
      }
    })

    return notificationTopic
  }

  private createS3Buckets() {
    const batchBucket = new s3.Bucket(this, "BatchInputBucket", {
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      autoDeleteObjects: true,
    })
    batchBucket.enableEventBridgeNotification()

    new cdk.CfnOutput(this, "BatchInputBucketName", {
      value: batchBucket.bucketName,
      description: "Name of input bucket to send PDF documents that Textract will read.",
    })

    const manifestFileCreatedRule = new eventBridge.Rule(this, "ManifestFileCreatedRule", {
      eventPattern: {
        source: ["aws.s3"],
        detailType: ["Object Created"],
        detail: {
          bucket: {
            name: [batchBucket.bucketName],
          },
          object: {
            key: ["manifest.json"],
          },
        },
      },
    })

    return { batchBucket, manifestFileCreatedRule }
  }

  /**
   * This is the lambda function that will create the Bedrock inference file, and upload it to S3.
   * See `src/bedrock-batcher/handler.py` for the implementation.
   *
   */
  private createPrepareBedrockBatchLambda(
    textractOutputBucket: s3.IBucket,
    bedrockBucket: s3.IBucket
  ) {
    const fn = new lambda.Function(this, "PrepareBedrockBatchLambda", {
      runtime: LAMBDA_PYTHON_VERSION,
      code: lambda.Code.fromAsset("src/bedrock-batcher/"),
      handler: "handler.create_bedrock_batch_file",
      timeout: cdk.Duration.minutes(5),
      memorySize: 1024,
      architecture: lambda.Architecture.ARM_64,
      layers: [this.LPTLayer],
      environment: {
        POWERTOOLS_SERVICE_NAME: "bedrock-batcher",
        POWERTOOLS_LOG_LEVEL: "INFO",
        OUTPUT_BUCKET: bedrockBucket.bucketName,
        BEDROCK_BATCH_RESULTS_PREFIX,
      },
    })

    // Allow the Lambda to read the textract results. It needs this to create the Bedrock prompts
    // that go into the batch inference file.
    const textractResultsReadPolicy = new iam.Policy(this, "TextractResultsReadPolicy", {
      statements: [
        new iam.PolicyStatement({
          actions: ["s3:PutObject", "s3:GetObject"],
          resources: [textractOutputBucket.arnForObjects("*")],
        }),
      ],
    })
    fn.role?.attachInlinePolicy(textractResultsReadPolicy)

    // This allows the Lambda to write the batch inference file to the correct bucket.
    const bedrockBucketPolicy = new iam.Policy(this, "BatchFilePutObjectPolicy", {
      statements: [
        new iam.PolicyStatement({
          actions: ["s3:PutObject"],
          resources: [bedrockBucket.arnForObjects("*")],
        }),
      ],
    })
    fn.role?.attachInlinePolicy(bedrockBucketPolicy)

    return fn
  }

  /**
   * Create a Bedrock role that is used for the batch inference job. This is the role that is
   * passed to the Step Function workflow so that it can call the Bedrock inference.
   *
   * This role needs to allow both calling Bedrock, and read/write to the S3 bucket.
   */
  private createBedrockBatchServiceRole(bedrockBucket: s3.IBucket): iam.Role {
    const bedrockRole = new iam.Role(this, "BedrockBatchServiceRole", {
      assumedBy: new iam.ServicePrincipal("bedrock.amazonaws.com"),
      description: "Role for Amazon Bedrock batch operations",
    })

    // Add Bedrock permissions
    bedrockRole.addToPolicy(
      new iam.PolicyStatement({
        sid: "BatchInference",
        effect: iam.Effect.ALLOW,
        actions: [
          "bedrock:InvokeModel",
          "bedrock:ListFoundationModels",
          "bedrock:GetFoundationModel",
          "bedrock:TagResource",
          "bedrock:UntagResource",
          "bedrock:ListTagsForResource",
          "bedrock:CreateModelInvocationJob",
          "bedrock:GetModelInvocationJob",
          "bedrock:ListModelInvocationJobs",
          "bedrock:StopModelInvocationJob",
        ],
        resources: ["*"],
      })
    )

    const s3Policy = new iam.Policy(this, "S3BedrockBatchAccess", {
      statements: [
        new iam.PolicyStatement({
          sid: "S3BedrockBatchAccess",
          effect: iam.Effect.ALLOW,
          actions: ["s3:PutObject", "s3:ListBucket", "s3:GetObject"],
          resources: [bedrockBucket.bucketArn, bedrockBucket.arnForObjects("*")],
        }),
      ],
    })
    bedrockRole.attachInlinePolicy(s3Policy)

    return bedrockRole
  }

  /**
   * Create an EventBridge rule for batch inference completed state changes
   */
  private createBedrockBatchCompletedRule() {
    return new eventBridge.Rule(this, "BedrockJobCompletedRule", {
      eventPattern: {
        source: ["aws.bedrock"],
        detailType: ["Batch Inference Job State Change"],
        detail: {
          status: ["Completed"],
        },
      },
    })
  }

  /**
   * Create an EventBridge rule for any batch inference state changes
   */
  private createBedrockStateChangeRule() {
    return new eventBridge.Rule(this, "BedrockStateChangeRule", {
      eventPattern: {
        source: ["aws.bedrock"],
        detailType: ["Batch Inference Job State Change"],
      },
    })
  }

  private createCloudWatchDebugLogGroup() {
    return new cloudwatch.LogGroup(this, "S3CatchAllLogGroup", {
      logGroupName: `/aws/events/${this.stackName}-catchall`,
      retention: cloudwatch.RetentionDays.ONE_WEEK,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    })
  }
}
