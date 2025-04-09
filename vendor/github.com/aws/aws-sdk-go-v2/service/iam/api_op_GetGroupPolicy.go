// Code generated by smithy-go-codegen DO NOT EDIT.

package iam

import (
	"context"
	"fmt"
	awsmiddleware "github.com/aws/aws-sdk-go-v2/aws/middleware"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

// Retrieves the specified inline policy document that is embedded in the
// specified IAM group.
//
// Policies returned by this operation are URL-encoded compliant with [RFC 3986]. You can
// use a URL decoding method to convert the policy back to plain JSON text. For
// example, if you use Java, you can use the decode method of the
// java.net.URLDecoder utility class in the Java SDK. Other languages and SDKs
// provide similar functionality.
//
// An IAM group can also have managed policies attached to it. To retrieve a
// managed policy document that is attached to a group, use GetPolicyto determine the
// policy's default version, then use GetPolicyVersionto retrieve the policy document.
//
// For more information about policies, see [Managed policies and inline policies] in the IAM User Guide.
//
// [RFC 3986]: https://tools.ietf.org/html/rfc3986
// [Managed policies and inline policies]: https://docs.aws.amazon.com/IAM/latest/UserGuide/policies-managed-vs-inline.html
func (c *Client) GetGroupPolicy(ctx context.Context, params *GetGroupPolicyInput, optFns ...func(*Options)) (*GetGroupPolicyOutput, error) {
	if params == nil {
		params = &GetGroupPolicyInput{}
	}

	result, metadata, err := c.invokeOperation(ctx, "GetGroupPolicy", params, optFns, c.addOperationGetGroupPolicyMiddlewares)
	if err != nil {
		return nil, err
	}

	out := result.(*GetGroupPolicyOutput)
	out.ResultMetadata = metadata
	return out, nil
}

type GetGroupPolicyInput struct {

	// The name of the group the policy is associated with.
	//
	// This parameter allows (through its [regex pattern]) a string of characters consisting of upper
	// and lowercase alphanumeric characters with no spaces. You can also include any
	// of the following characters: _+=,.@-
	//
	// [regex pattern]: http://wikipedia.org/wiki/regex
	//
	// This member is required.
	GroupName *string

	// The name of the policy document to get.
	//
	// This parameter allows (through its [regex pattern]) a string of characters consisting of upper
	// and lowercase alphanumeric characters with no spaces. You can also include any
	// of the following characters: _+=,.@-
	//
	// [regex pattern]: http://wikipedia.org/wiki/regex
	//
	// This member is required.
	PolicyName *string

	noSmithyDocumentSerde
}

// Contains the response to a successful GetGroupPolicy request.
type GetGroupPolicyOutput struct {

	// The group the policy is associated with.
	//
	// This member is required.
	GroupName *string

	// The policy document.
	//
	// IAM stores policies in JSON format. However, resources that were created using
	// CloudFormation templates can be formatted in YAML. CloudFormation always
	// converts a YAML policy to JSON format before submitting it to IAM.
	//
	// This member is required.
	PolicyDocument *string

	// The name of the policy.
	//
	// This member is required.
	PolicyName *string

	// Metadata pertaining to the operation's result.
	ResultMetadata middleware.Metadata

	noSmithyDocumentSerde
}

func (c *Client) addOperationGetGroupPolicyMiddlewares(stack *middleware.Stack, options Options) (err error) {
	if err := stack.Serialize.Add(&setOperationInputMiddleware{}, middleware.After); err != nil {
		return err
	}
	err = stack.Serialize.Add(&awsAwsquery_serializeOpGetGroupPolicy{}, middleware.After)
	if err != nil {
		return err
	}
	err = stack.Deserialize.Add(&awsAwsquery_deserializeOpGetGroupPolicy{}, middleware.After)
	if err != nil {
		return err
	}
	if err := addProtocolFinalizerMiddlewares(stack, options, "GetGroupPolicy"); err != nil {
		return fmt.Errorf("add protocol finalizers: %v", err)
	}

	if err = addlegacyEndpointContextSetter(stack, options); err != nil {
		return err
	}
	if err = addSetLoggerMiddleware(stack, options); err != nil {
		return err
	}
	if err = addClientRequestID(stack); err != nil {
		return err
	}
	if err = addComputeContentLength(stack); err != nil {
		return err
	}
	if err = addResolveEndpointMiddleware(stack, options); err != nil {
		return err
	}
	if err = addComputePayloadSHA256(stack); err != nil {
		return err
	}
	if err = addRetry(stack, options); err != nil {
		return err
	}
	if err = addRawResponseToMetadata(stack); err != nil {
		return err
	}
	if err = addRecordResponseTiming(stack); err != nil {
		return err
	}
	if err = addSpanRetryLoop(stack, options); err != nil {
		return err
	}
	if err = addClientUserAgent(stack, options); err != nil {
		return err
	}
	if err = smithyhttp.AddErrorCloseResponseBodyMiddleware(stack); err != nil {
		return err
	}
	if err = smithyhttp.AddCloseResponseBodyMiddleware(stack); err != nil {
		return err
	}
	if err = addSetLegacyContextSigningOptionsMiddleware(stack); err != nil {
		return err
	}
	if err = addTimeOffsetBuild(stack, c); err != nil {
		return err
	}
	if err = addUserAgentRetryMode(stack, options); err != nil {
		return err
	}
	if err = addOpGetGroupPolicyValidationMiddleware(stack); err != nil {
		return err
	}
	if err = stack.Initialize.Add(newServiceMetadataMiddleware_opGetGroupPolicy(options.Region), middleware.Before); err != nil {
		return err
	}
	if err = addRecursionDetection(stack); err != nil {
		return err
	}
	if err = addRequestIDRetrieverMiddleware(stack); err != nil {
		return err
	}
	if err = addResponseErrorMiddleware(stack); err != nil {
		return err
	}
	if err = addRequestResponseLogging(stack, options); err != nil {
		return err
	}
	if err = addDisableHTTPSMiddleware(stack, options); err != nil {
		return err
	}
	if err = addSpanInitializeStart(stack); err != nil {
		return err
	}
	if err = addSpanInitializeEnd(stack); err != nil {
		return err
	}
	if err = addSpanBuildRequestStart(stack); err != nil {
		return err
	}
	if err = addSpanBuildRequestEnd(stack); err != nil {
		return err
	}
	return nil
}

func newServiceMetadataMiddleware_opGetGroupPolicy(region string) *awsmiddleware.RegisterServiceMetadata {
	return &awsmiddleware.RegisterServiceMetadata{
		Region:        region,
		ServiceID:     ServiceID,
		OperationName: "GetGroupPolicy",
	}
}
