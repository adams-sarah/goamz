package dynamodb

import (
	"errors"
	"fmt"
	simplejson "github.com/bitly/go-simplejson"
)

func (t *Table) Query(attributeComparisons []AttributeComparison) ([]map[string]*Attribute, error) {
	q := NewQuery(t)
	q.AddKeyConditions(attributeComparisons)
	results, lastKey, err := runQuery(q, t)
	if lastKey != nil {
		return results, errors.New("Response size exceeds limit of 1 MB. Use PaginatedQuery instead.")
	}
	return results, err
}

func (t *Table) LimitedQuery(attributeComparisons []AttributeComparison, limit int64) ([]map[string]*Attribute, error) {
	q := NewQuery(t)
	q.AddKeyConditions(attributeComparisons)
	q.AddLimit(limit)
	results, lastKey, err := runQuery(q, t)
	if lastKey != nil {
		return results, errors.New("Response size exceeds limit of 1 MB. Use PaginatedQuery instead.")
	}
	return results, err
}

func (t *Table) PaginatedQuery(attributeComparisons []AttributeComparison, startKey map[string]*Attribute) ([]map[string]*Attribute, map[string]*Attribute, error) {
	q := NewQuery(t)
	q.AddKeyConditions(attributeComparisons)
	if startKey != nil {
		q.AddExclusiveStartKey(startKey)
	}
	return runQuery(q, t)
}

func (t *Table) CountQuery(attributeComparisons []AttributeComparison) (int64, error) {
	q := NewQuery(t)
	q.AddKeyConditions(attributeComparisons)
	q.AddSelect("COUNT")
	jsonResponse, err := t.Server.queryServer("DynamoDB_20120810.Query", q)
	if err != nil {
		return 0, err
	}
	json, err := simplejson.NewJson(jsonResponse)
	if err != nil {
		return 0, err
	}

	itemCount, err := json.Get("Count").Int64()
	if err != nil {
		return 0, err
	}

	return itemCount, nil
}

func runQuery(q *Query, t *Table) ([]map[string]*Attribute, map[string]*Attribute, error) {
	var lastKey map[string]*Attribute


	jsonResponse, err := t.Server.queryServer("DynamoDB_20120810.Query", q)
	if err != nil {
		return nil, lastKey, err
	}

	json, err := simplejson.NewJson(jsonResponse)
	if err != nil {
		return nil, lastKey, err
	}

	itemCount, err := json.Get("Count").Int()
	if err != nil {
		message := fmt.Sprintf("Unexpected response %s", jsonResponse)
		return nil, lastKey, errors.New(message)
	}

	results := make([]map[string]*Attribute, itemCount)

	for i, _ := range results {
		item, err := json.Get("Items").GetIndex(i).Map()
		if err != nil {
			fmt.Println(err)
			message := fmt.Sprintf("Unexpected response %s", jsonResponse)
			return nil, lastKey, errors.New(message)
		}
		results[i] = parseAttributes(item)
	}

	if lastKeyJson, ok := json.CheckGet("LastEvaluatedKey"); ok {
		lastEvaluatedKey, err := lastKeyJson.Map()
		if err != nil {
			message := fmt.Sprintf("Unexpected response %s", jsonResponse)
			return nil, lastKey, errors.New(message)
		}
		lastKey = parseAttributes(lastEvaluatedKey)
	}

	return results, lastKey, nil
}
