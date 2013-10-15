package dynamodb_test

import (
	"github.com/crowdmob/goamz/aws"
	"github.com/crowdmob/goamz/dynamodb"
	"testing"
	"log"
)

func TestPaginatedQuery(t *testing.T) {
	if !*amazon {
		t.Log("Amazon tests not enabled")
		return
	}
	auth, err := aws.EnvAuth()

	if err != nil {
		t.Log(err)
		t.FailNow()
	}

	server := dynamodb.Server{auth, aws.USEast}
	primary := dynamodb.NewStringAttribute("DeviceSeries", "")
	secondary := dynamodb.NewNumericAttribute("Recorded", "")
	key := dynamodb.PrimaryKey{primary, secondary}
	table := server.NewTable("SeriesData", key)

	conditions := []dynamodb.AttributeComparison{
		*dynamodb.NewStringAttributeComparison("DeviceSeries", dynamodb.COMPARISON_EQUAL, "1:1"),
	}

	results, lastKey, err := table.PaginatedQuery(conditions, nil)
	if err != nil {
		log.Printf("Error from FetchResults: %#v", err)
	}
	log.Println(len(results))
	log.Println(lastKey)

	results, lastKey, err = table.PaginatedQuery(conditions, lastKey)
	if err != nil {
		log.Printf("Error from FetchResults: %#v", err)
	}
	log.Println(len(results))
	log.Println(lastKey)
}