package main

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func TestNextPartNumberMarkerUsesServiceMarker(t *testing.T) {
	marker, err := nextPartNumberMarker("1000", nil, aws.String("2000"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if marker != "2000" {
		t.Fatalf("expected marker 2000, got %q", marker)
	}
}

func TestNextPartNumberMarkerFallsBackToLastPart(t *testing.T) {
	parts := []s3types.Part{
		{PartNumber: aws.Int32(1001)},
		{PartNumber: aws.Int32(1002)},
	}

	marker, err := nextPartNumberMarker("1000", parts, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if marker != "1002" {
		t.Fatalf("expected marker 1002, got %q", marker)
	}
}

func TestNextPartNumberMarkerErrorsWhenTruncatedPageHasNoParts(t *testing.T) {
	_, err := nextPartNumberMarker("1000", nil, nil)
	if err == nil {
		t.Fatalf("expected an error")
	}
}

func TestNextPartNumberMarkerErrorsWhenMarkerDoesNotAdvance(t *testing.T) {
	_, err := nextPartNumberMarker("1000", nil, aws.String("1000"))
	if err == nil {
		t.Fatalf("expected an error")
	}
}
