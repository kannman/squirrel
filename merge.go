package squirrel

import (
	"bytes"
	"database/sql"
	"fmt"
	"strings"

	"github.com/lann/builder"
)

type mergeData struct {
	PlaceholderFormat PlaceholderFormat
	RunWith           BaseRunner
	Prefixes          exprs
	Table             string
	Using             Sqlizer
	On                *wherePart
	WhenMatched       Sqlizer
	WhenNotMatched    Sqlizer
	Suffixes          exprs
	Output            []string
	OutputInto        string
}

func (d *mergeData) Exec() (sql.Result, error) {
	if d.RunWith == nil {
		return nil, RunnerNotSet
	}
	return ExecWith(d.RunWith, d)
}

func (d *mergeData) Query() (*sql.Rows, error) {
	if d.RunWith == nil {
		return nil, RunnerNotSet
	}
	return QueryWith(d.RunWith, d)
}

func (d *mergeData) QueryRow() RowScanner {
	if d.RunWith == nil {
		return &Row{err: RunnerNotSet}
	}
	queryRower, ok := d.RunWith.(QueryRower)
	if !ok {
		return &Row{err: RunnerNotQueryRunner}
	}
	return QueryRowWith(queryRower, d)
}

func (d *mergeData) ToSql() (sqlStr string, args []interface{}, err error) {
	if len(d.Table) == 0 {
		err = fmt.Errorf("merge statements must specify a target table")
		return
	}
	if d.Using == nil {
		err = fmt.Errorf("merge statements must specify a using statement")
		return
	}
	if d.On == nil {
		err = fmt.Errorf("merge statements must specify an on statement")
		return
	}

	sql := &bytes.Buffer{}

	if len(d.Prefixes) > 0 {
		args, _ = d.Prefixes.AppendToSql(sql, " ", args)
		sql.WriteString(" ")
	}

	sql.WriteString("MERGE INTO ")
	sql.WriteString(d.Table)

	sql.WriteString(" USING ")
	args, err = appendToSql([]Sqlizer{d.Using}, sql, "", args)
	if err != nil {
		return
	}

	sql.WriteString(" ON ")
	args, err = appendToSql([]Sqlizer{d.On}, sql, "", args)
	if err != nil {
		return
	}

	if d.WhenMatched != nil {
		sql.WriteString(" WHEN MATCHED THEN ")

		args, err = appendToSql([]Sqlizer{d.WhenMatched}, sql, "", args)
		if err != nil {
			return
		}
	}
	if d.WhenNotMatched != nil {
		sql.WriteString(" WHEN NOT MATCHED THEN ")

		args, err = appendToSql([]Sqlizer{d.WhenNotMatched}, sql, "", args)
		if err != nil {
			return
		}
	}

	if len(d.Output) > 0 {
		sql.WriteString(" OUTPUT ")
		sql.WriteString(fmt.Sprintf("%s", strings.Join(d.Output, ",")))
	}

	if d.OutputInto != "" {
		sql.WriteString(" INTO ")
		sql.WriteString(d.OutputInto)
		sql.WriteString("")
	}

	if len(d.Suffixes) > 0 {
		sql.WriteString(" ")
		args, _ = d.Suffixes.AppendToSql(sql, " ", args)
	}

	sql.WriteString(";")

	sqlStr, err = d.PlaceholderFormat.ReplacePlaceholders(sql.String())
	return
}

// Builder

// MergeBuilder builds SQL UPDATE statements.
type MergeBuilder builder.Builder

func init() {
	builder.Register(MergeBuilder{}, mergeData{})
}

// Format methods

// PlaceholderFormat sets PlaceholderFormat (e.g. Question or Dollar) for the
// query.
func (b MergeBuilder) PlaceholderFormat(f PlaceholderFormat) MergeBuilder {
	return builder.Set(b, "PlaceholderFormat", f).(MergeBuilder)
}

// SQL methods

// ToSql builds the query into a SQL string and bound args.
func (b MergeBuilder) ToSql() (string, []interface{}, error) {
	data := builder.GetStruct(b).(mergeData)
	return data.ToSql()
}

// Prefix adds an expression to the beginning of the query
func (b MergeBuilder) Prefix(sql string, args ...interface{}) MergeBuilder {
	return builder.Append(b, "Prefixes", Expr(sql, args...)).(MergeBuilder)
}

// Table sets the table to be updated.
func (b MergeBuilder) Table(table string) MergeBuilder {
	return builder.Set(b, "Table", table).(MergeBuilder)
}

// Suffix adds an expression to the end of the query.
func (b MergeBuilder) Suffix(sql string, args ...interface{}) MergeBuilder {
	return builder.Append(b, "Suffixes", Expr(sql, args...)).(MergeBuilder)
}

// Output adds insert output columns.
func (b MergeBuilder) OutputInto(into string, args ...interface{}) MergeBuilder {
	return builder.Set(b, "OutputInto", into).(MergeBuilder).Output(args...)
}

// Output adds insert output columns.
func (b MergeBuilder) Output(args ...interface{}) MergeBuilder {
	return builder.Append(b, "Output", args...).(MergeBuilder)
}

// Using adds Using part.
func (b MergeBuilder) Using(expr Sqlizer) MergeBuilder {
	return builder.Set(b, "Using", expr).(MergeBuilder)
}

// On adds On part.
func (b MergeBuilder) On(pred interface{}, args ...interface{}) MergeBuilder {
	return builder.Set(b, "On", newWherePart(pred, args...)).(MergeBuilder)
}

// On adds WhenMatched part.
func (b MergeBuilder) WhenMatched(expr Sqlizer) MergeBuilder {
	return builder.Set(b, "WhenMatched", expr).(MergeBuilder)
}

// On adds WhenNotMatched part.
func (b MergeBuilder) WhenNotMatched(expr Sqlizer) MergeBuilder {
	return builder.Set(b, "WhenNotMatched", expr).(MergeBuilder)
}
