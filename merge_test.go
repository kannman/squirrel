package squirrel

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMergeBuilderToSql(t *testing.T) {
	b := Merge("test as t").
		Prefix("WITH prefix AS ?", 0).
		Using(Expr("(SELECT ? as ID) as s", 1)).
		On("s.ID = t.ID").
		WhenMatched(Expr("UPDATE SET c = ?", 2)).
		WhenNotMatched(Expr("INSERT(d) VALUES(?)", 3)).
		Suffix("RETURNING ?", 4)

	sql, args, err := b.ToSql()
	assert.NoError(t, err)

	expectedSql :=
		"WITH prefix AS ? " +
			"MERGE INTO test as t " +
			"USING (SELECT ? as ID) as s " +
			"ON s.ID = t.ID " +
			"WHEN MATCHED THEN UPDATE SET c = ? " +
			"WHEN NOT MATCHED THEN INSERT(d) VALUES(?) " +
			"RETURNING ?;"
	assert.Equal(t, expectedSql, sql)

	expectedArgs := []interface{}{0, 1, 2, 3, 4}
	assert.Equal(t, expectedArgs, args)
}
