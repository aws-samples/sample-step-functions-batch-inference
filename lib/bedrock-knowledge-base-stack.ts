import * as cdk from "aws-cdk-lib"
import * as bedrock from "aws-cdk-lib/aws-bedrock"
import * as cr from "aws-cdk-lib/custom-resources"
import * as iam from "aws-cdk-lib/aws-iam"
import * as lambda from "aws-cdk-lib/aws-lambda"
import * as opensearch from "aws-cdk-lib/aws-opensearchserverless"
import * as s3 from "aws-cdk-lib/aws-s3"
import { Construct } from "constructs"

const LAMBDA_PYTHON_VERSION = lambda.Runtime.PYTHON_3_13

export class BedrockKnowledgeBaseStack extends cdk.Stack {
  public readonly knowledgeBase: bedrock.CfnKnowledgeBase
  public readonly dataSource: bedrock.CfnDataSource
  public readonly textractOutputBucket: s3.Bucket
  public readonly bedrockBatchBucket: s3.Bucket

  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props)

    // Create S3 buckets that will be shared with main stack
    this.textractOutputBucket = new s3.Bucket(this, "TranscriptionOutputBucket", {
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      autoDeleteObjects: true,
    })

    this.bedrockBatchBucket = new s3.Bucket(this, "BedrockBatchBucket", {
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      autoDeleteObjects: true,
    })

    // Create Bedrock Knowledge Base infrastructure
    const { knowledgeBase, dataSource } = this.createKnowledgeBaseInfrastructure(
      this.textractOutputBucket
    )

    this.knowledgeBase = knowledgeBase
    this.dataSource = dataSource
  }

  /**
   * Create Bedrock Knowledge Base infrastructure with OpenSearch Serverless
   */
  private createKnowledgeBaseInfrastructure(textractOutputBucket: s3.IBucket) {
    // Create encryption security policy for OpenSearch Serverless
    const encryptionPolicy = new opensearch.CfnSecurityPolicy(this, "EncryptionPolicy", {
      name: "kb-collection-encryption-policy",
      type: "encryption",
      policy: JSON.stringify({
        Rules: [
          {
            ResourceType: "collection",
            Resource: ["collection/kb-collection"],
          },
        ],
        AWSOwnedKey: true,
      }),
    })

    // Create network security policy for OpenSearch Serverless
    const networkPolicy = new opensearch.CfnSecurityPolicy(this, "NetworkPolicy", {
      name: "kb-collection-network-policy",
      type: "network",
      policy: JSON.stringify([
        {
          Rules: [
            {
              ResourceType: "collection",
              Resource: ["collection/kb-collection"],
            },
            {
              ResourceType: "dashboard",
              Resource: ["collection/kb-collection"],
            },
          ],
          AllowFromPublic: true,
        },
      ]),
    })

    // Create execution role for Knowledge Base
    const kbExecutionRole = new iam.Role(this, "KnowledgeBaseExecutionRole", {
      assumedBy: new iam.ServicePrincipal("bedrock.amazonaws.com"),
      description: "Execution role for Bedrock Knowledge Base",
    })

    // Create execution role for the index creation Lambda
    const createIndexExecutionRole = new iam.Role(this, "CreateIndexExecutionRole", {
      assumedBy: new iam.ServicePrincipal("lambda.amazonaws.com"),
      description: "Execution role for Lambda that creates OpenSearch index",
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName("service-role/AWSLambdaBasicExecutionRole"),
      ],
    })

    // Create data access policy
    const principalArns = [
      `arn:aws:iam::${this.account}:root`,
      kbExecutionRole.roleArn,
      createIndexExecutionRole.roleArn,
    ]

    const dataAccessPolicy = new opensearch.CfnAccessPolicy(this, "DataAccessPolicy", {
      name: `${this.stackName}-kb-access`.toLowerCase(),
      type: "data",
      description: "Data access policy for knowledge base collection",
      policy: JSON.stringify([
        {
          Rules: [
            {
              ResourceType: "index",
              Resource: ["index/kb-collection/*"],
              Permission: [
                "aoss:UpdateIndex",
                "aoss:DescribeIndex",
                "aoss:ReadDocument",
                "aoss:WriteDocument",
                "aoss:CreateIndex",
              ],
            },
            {
              ResourceType: "collection",
              Resource: ["collection/kb-collection"],
              Permission: [
                "aoss:DescribeCollectionItems",
                "aoss:CreateCollectionItems",
                "aoss:UpdateCollectionItems",
              ],
            },
          ],
          Principal: principalArns,
        },
      ]),
    })

    // Create OpenSearch Serverless collection
    const vectorCollection = new opensearch.CfnCollection(this, "VectorCollection", {
      name: "kb-collection",
      type: "VECTORSEARCH",
      description: "Collection for knowledge base vectors",
    })

    // Ensure security policies are created before collection
    vectorCollection.addDependency(encryptionPolicy)
    vectorCollection.addDependency(networkPolicy)
    vectorCollection.addDependency(dataAccessPolicy)

    // Add policy for embeddings model access
    kbExecutionRole.addToPolicy(
      new iam.PolicyStatement({
        effect: iam.Effect.ALLOW,
        actions: ["bedrock:InvokeModel"],
        resources: [
          `arn:aws:bedrock:${this.region}::foundation-model/amazon.titan-embed-text-v2:0`,
        ],
      })
    )

    // Add OpenSearch Serverless permissions to Knowledge Base role
    kbExecutionRole.addToPolicy(
      new iam.PolicyStatement({
        effect: iam.Effect.ALLOW,
        actions: ["aoss:APIAccessAll"],
        resources: [vectorCollection.attrArn],
      })
    )

    // Add S3 permissions for Knowledge Base to read from textract output bucket
    kbExecutionRole.addToPolicy(
      new iam.PolicyStatement({
        effect: iam.Effect.ALLOW,
        actions: ["s3:GetObject", "s3:ListBucket"],
        resources: [textractOutputBucket.bucketArn, textractOutputBucket.arnForObjects("*")],
      })
    )

    // Add OpenSearch Serverless permissions to Lambda role
    createIndexExecutionRole.addToPolicy(
      new iam.PolicyStatement({
        effect: iam.Effect.ALLOW,
        actions: ["aoss:APIAccessAll"],
        resources: [vectorCollection.attrArn],
      })
    )

    // Create Lambda function to create the index
    const createIndexFunction = new lambda.Function(this, "CreateIndexFunction", {
      runtime: LAMBDA_PYTHON_VERSION,
      handler: "lambda_function.lambda_handler",
      code: lambda.Code.fromAsset("src/create-index", {
        bundling: {
          image: lambda.Runtime.PYTHON_3_13.bundlingImage,
          command: [
            "bash",
            "-c",
            "pip install -r requirements.txt -t /asset-output && cp -au . /asset-output",
          ],
        },
      }),
      role: createIndexExecutionRole,
      timeout: cdk.Duration.minutes(15),
      memorySize: 512,
      architecture: lambda.Architecture.ARM_64,
      environment: {
        COLLECTION_HOST: vectorCollection.attrCollectionEndpoint,
        VECTOR_INDEX_NAME: "knowledge-base-index",
        VECTOR_FIELD_NAME: "vector",
        REGION_NAME: this.region,
      },
    })

    // Create custom resource to trigger index creation
    const indexCreationProvider = new cr.Provider(this, "IndexCreationProvider", {
      onEventHandler: createIndexFunction,
    })

    const indexCreationResource = new cdk.CustomResource(this, "IndexCreationResource", {
      serviceToken: indexCreationProvider.serviceToken,
    })

    // Ensure index creation happens after collection is ready
    indexCreationResource.node.addDependency(vectorCollection)

    // Create the Knowledge Base with proper storage configuration
    const knowledgeBase = new bedrock.CfnKnowledgeBase(this, "ResearchPapersKnowledgeBase", {
      name: `research-papers-${this.stackName}-${this.node.addr}`,
      description: "Knowledge base for research papers analysis",
      roleArn: kbExecutionRole.roleArn,
      knowledgeBaseConfiguration: {
        type: "VECTOR",
        vectorKnowledgeBaseConfiguration: {
          embeddingModelArn: `arn:aws:bedrock:${this.region}::foundation-model/amazon.titan-embed-text-v2:0`,
        },
      },
      storageConfiguration: {
        type: "OPENSEARCH_SERVERLESS",
        opensearchServerlessConfiguration: {
          collectionArn: vectorCollection.attrArn,
          vectorIndexName: "knowledge-base-index",
          fieldMapping: {
            vectorField: "vector",
            textField: "text",
            metadataField: "metadata",
          },
        },
      },
    })

    // Knowledge Base depends on the index being created
    knowledgeBase.node.addDependency(indexCreationResource)

    // Create the Data Source for S3-based ingestion
    const dataSource = new bedrock.CfnDataSource(this, "ResearchPapersDataSource", {
      knowledgeBaseId: knowledgeBase.attrKnowledgeBaseId,
      name: "research-papers-datasource",
      description: "Data source for S3-based document ingestion",
      // Set deletion policy to RETAIN on data source to make stack deletions easier
      dataDeletionPolicy: "RETAIN",
      dataSourceConfiguration: {
        type: "S3",
        s3Configuration: {
          bucketArn: textractOutputBucket.bucketArn,
        },
      },
    })

    return { knowledgeBase, dataSource }
  }
}
