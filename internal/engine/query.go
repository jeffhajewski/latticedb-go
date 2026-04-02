package engine

import (
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strconv"
	"strings"

	"github.com/jeffhajewski/latticedb-go/internal/search"
	"github.com/jeffhajewski/latticedb-go/internal/store"
)

type queryPlan struct {
	unwindClause  *unwindClause
	matchPatterns []matchPattern
	whereClauses  []*whereClause
	setClause     *setClause
	createClause  *createClause
	removeClause  *removeClause
	deleteClause  *deleteClause
	returnClause  *returnClause
	limit         int
}

type matchPattern interface {
	apply(tx *Tx, rows []queryRow) ([]queryRow, error)
}

type nodePattern struct {
	Var        string
	Labels     []string
	Properties map[string]any
}

type edgePattern struct {
	Left     nodePattern
	EdgeVar  string
	EdgeType string
	Right    nodePattern
}

type whereClause struct {
	Kind     whereKind
	Var      string
	Property string
	Expr     valueExpr
}

type whereKind string

const (
	whereEquals    whereKind = "equals"
	whereIsNull    whereKind = "is_null"
	whereIsNotNull whereKind = "is_not_null"
	whereVector    whereKind = "vector"
	whereFTS       whereKind = "fts"
	whereBindingID whereKind = "binding_id"
)

type setClause struct {
	Kind     setKind
	Var      string
	Property string
	Expr     valueExpr
}

type createClause struct {
	SourceVar string
	TargetVar string
	EdgeType  string
	Props     map[string]valueExpr
}

type setKind string

const (
	setProperty setKind = "property"
	setReplace  setKind = "replace"
	setMerge    setKind = "merge"
)

type removeClause struct {
	Items []removeItem
}

type removeItem struct {
	Var      string
	Property string
	Label    string
}

type deleteClause struct {
	Vars []string
}

type unwindClause struct {
	Expr valueExpr
	Var  string
}

type returnClause struct {
	CountVar    string
	CountAlias  string
	Projections []projection
}

type projection struct {
	Kind     projectionKind
	Var      string
	Property string
	Alias    string
}

type projectionKind string

const (
	projectionProperty  projectionKind = "property"
	projectionBindingID projectionKind = "binding_id"
	projectionValue     projectionKind = "value"
)

type queryRow struct {
	Bindings map[string]boundValue
	Order    float64
}

type boundValue struct {
	Node     *store.NodeRecord
	Edge     *store.EdgeRecord
	Value    any
	HasValue bool
}

type valueExpr interface {
	eval(row queryRow, params map[string]any) (any, error)
}

type literalExpr struct {
	Value any
}

type mapLiteralExpr struct {
	Entries map[string]valueExpr
}

type paramExpr struct {
	Name string
}

type variableExpr struct {
	Name string
}

func parseQuery(query string) (*queryPlan, error) {
	query = strings.TrimSpace(query)
	switch {
	case strings.HasPrefix(query, "MATCH "):
		return parseMatchQuery(query)
	case strings.HasPrefix(query, "UNWIND "):
		return parseUnwindQuery(query)
	default:
		return nil, fmt.Errorf("unsupported query %q", query)
	}
}

func parseMatchQuery(query string) (*queryPlan, error) {
	rest := strings.TrimSpace(strings.TrimPrefix(query, "MATCH "))
	matchText, nextKeyword, tail := splitOnNextClause(rest, " WHERE ", " RETURN ", " SET ", " CREATE ", " REMOVE ", " DELETE ")
	patterns, err := parseMatchPatterns(matchText)
	if err != nil {
		return nil, err
	}

	plan := &queryPlan{matchPatterns: patterns}

	switch nextKeyword {
	case " WHERE ":
		whereText, whereNext, afterWhere := splitOnNextClause(tail, " RETURN ", " SET ", " CREATE ", " REMOVE ", " DELETE ")
		whereClauses, err := parseWhereClauses(whereText)
		if err != nil {
			return nil, err
		}
		plan.whereClauses = whereClauses
		nextKeyword = whereNext
		tail = afterWhere
	case " RETURN ", " SET ", " CREATE ", " REMOVE ", " DELETE ", "":
	default:
		return nil, fmt.Errorf("unsupported clause after MATCH: %q", nextKeyword)
	}

	switch nextKeyword {
	case " RETURN ":
		returnText, limitText, hasLimit := splitLimitClause(tail)
		returnClause, err := parseReturnClause(returnText)
		if err != nil {
			return nil, err
		}
		plan.returnClause = returnClause
		if hasLimit {
			limit, err := strconv.Atoi(strings.TrimSpace(limitText))
			if err != nil {
				return nil, fmt.Errorf("invalid LIMIT %q", limitText)
			}
			plan.limit = limit
		}
	case " SET ":
		setClause, err := parseSetClause(tail)
		if err != nil {
			return nil, err
		}
		plan.setClause = setClause
	case " CREATE ":
		createClause, err := parseCreateClause(tail)
		if err != nil {
			return nil, err
		}
		plan.createClause = createClause
	case " REMOVE ":
		removeClause, err := parseRemoveClause(tail)
		if err != nil {
			return nil, err
		}
		plan.removeClause = removeClause
	case " DELETE ":
		deleteClause, err := parseDeleteClause(tail)
		if err != nil {
			return nil, err
		}
		plan.deleteClause = deleteClause
	case "":
	default:
		return nil, fmt.Errorf("unsupported terminal clause %q", nextKeyword)
	}

	return plan, nil
}

func parseUnwindQuery(query string) (*queryPlan, error) {
	rest := strings.TrimSpace(strings.TrimPrefix(query, "UNWIND "))
	exprText, keyword, tail := splitOnNextClause(rest, " AS ")
	if keyword == "" {
		return nil, fmt.Errorf("invalid UNWIND clause %q", query)
	}
	expr, err := parseValueExpr(exprText)
	if err != nil {
		return nil, err
	}

	varName, nextKeyword, afterVar := splitOnNextClause(tail, " RETURN ")
	varName = strings.TrimSpace(varName)
	if varName == "" {
		return nil, fmt.Errorf("invalid UNWIND binding %q", query)
	}
	if nextKeyword != " RETURN " {
		return nil, fmt.Errorf("unsupported terminal clause %q", nextKeyword)
	}

	returnText, limitText, hasLimit := splitLimitClause(afterVar)
	returnClause, err := parseReturnClause(returnText)
	if err != nil {
		return nil, err
	}

	plan := &queryPlan{
		unwindClause: &unwindClause{
			Expr: expr,
			Var:  varName,
		},
		returnClause: returnClause,
	}
	if hasLimit {
		limit, err := strconv.Atoi(strings.TrimSpace(limitText))
		if err != nil {
			return nil, fmt.Errorf("invalid LIMIT %q", limitText)
		}
		plan.limit = limit
	}
	return plan, nil
}

func (plan *queryPlan) mutates() bool {
	return plan.setClause != nil || plan.createClause != nil || plan.removeClause != nil || plan.deleteClause != nil
}

func (plan *queryPlan) execute(tx *Tx, params map[string]any) (QueryResult, error) {
	rows := []queryRow{{Bindings: map[string]boundValue{}}}
	if plan.unwindClause != nil {
		var err error
		rows, err = plan.unwindClause.apply(rows, params)
		if err != nil {
			return QueryResult{}, err
		}
	}
	for _, pattern := range plan.matchPatterns {
		var err error
		rows, err = pattern.apply(tx, rows)
		if err != nil {
			return QueryResult{}, err
		}
	}

	if len(plan.whereClauses) > 0 {
		for _, clause := range plan.whereClauses {
			var err error
			rows, err = clause.apply(rows, params)
			if err != nil {
				return QueryResult{}, err
			}
		}
	}

	if plan.createClause != nil {
		if err := plan.createClause.apply(tx, rows, params); err != nil {
			return QueryResult{}, err
		}
	}
	if plan.setClause != nil {
		if err := plan.setClause.apply(rows, params); err != nil {
			return QueryResult{}, err
		}
	}
	if plan.removeClause != nil {
		if err := plan.removeClause.apply(rows); err != nil {
			return QueryResult{}, err
		}
	}
	if plan.deleteClause != nil {
		if err := plan.deleteClause.apply(tx, rows); err != nil {
			return QueryResult{}, err
		}
	}

	if plan.returnClause == nil {
		return QueryResult{}, nil
	}
	if plan.limit > 0 && len(rows) > plan.limit {
		rows = rows[:plan.limit]
	}
	return plan.returnClause.render(rows)
}

func (clause *unwindClause) apply(rows []queryRow, params map[string]any) ([]queryRow, error) {
	nextRows := make([]queryRow, 0)
	for _, row := range rows {
		value, err := clause.Expr.eval(row, params)
		if err != nil {
			return nil, err
		}
		list, ok := value.([]any)
		if !ok {
			return nil, fmt.Errorf("UNWIND requires list value, got %T", value)
		}
		for _, item := range list {
			nextRow := row.clone()
			nextRow.Bindings[clause.Var] = boundValue{
				Value:    store.CloneValue(item),
				HasValue: true,
			}
			nextRows = append(nextRows, nextRow)
		}
	}
	return nextRows, nil
}

func parseMatchPatterns(text string) ([]matchPattern, error) {
	parts := splitTopLevel(text, ',')
	patterns := make([]matchPattern, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		if strings.Contains(part, "->") {
			pattern, err := parseEdgePattern(part)
			if err != nil {
				return nil, err
			}
			patterns = append(patterns, pattern)
			continue
		}
		pattern, err := parseNodePattern(part)
		if err != nil {
			return nil, err
		}
		patterns = append(patterns, pattern)
	}
	return patterns, nil
}

func parseNodePattern(text string) (nodePattern, error) {
	body, err := trimEnclosed(text, '(', ')')
	if err != nil {
		return nodePattern{}, err
	}

	props := map[string]any{}
	propStart := findTopLevelRune(body, '{')
	prefix := strings.TrimSpace(body)
	if propStart >= 0 {
		propEnd := findMatchingBrace(body, propStart, '{', '}')
		if propEnd < 0 {
			return nodePattern{}, fmt.Errorf("unterminated node property map in %q", text)
		}
		parsedProps, err := parsePropertyLiteralMap(body[propStart+1 : propEnd])
		if err != nil {
			return nodePattern{}, err
		}
		props = parsedProps
		prefix = strings.TrimSpace(body[:propStart])
	}

	segments := strings.Split(prefix, ":")
	pattern := nodePattern{Properties: props}
	if len(segments) > 0 {
		first := strings.TrimSpace(segments[0])
		if first != "" {
			pattern.Var = first
		}
	}
	for _, segment := range segments[1:] {
		label := strings.TrimSpace(segment)
		if label != "" {
			pattern.Labels = append(pattern.Labels, label)
		}
	}
	return pattern, nil
}

func parseEdgePattern(text string) (edgePattern, error) {
	leftEnd := strings.Index(text, ")-[")
	if leftEnd < 0 {
		return edgePattern{}, fmt.Errorf("invalid edge pattern %q", text)
	}
	rightStart := strings.Index(text[leftEnd+3:], "]->")
	if rightStart < 0 {
		return edgePattern{}, fmt.Errorf("invalid edge pattern %q", text)
	}
	rightStart += leftEnd + 3

	left, err := parseNodePattern(text[:leftEnd+1])
	if err != nil {
		return edgePattern{}, err
	}
	right, err := parseNodePattern(text[rightStart+3:])
	if err != nil {
		return edgePattern{}, err
	}

	edgeBody := strings.TrimSpace(text[leftEnd+3 : rightStart])
	edgeSegments := strings.SplitN(edgeBody, ":", 2)
	pattern := edgePattern{Left: left, Right: right}
	if len(edgeSegments) == 2 {
		pattern.EdgeVar = strings.TrimSpace(edgeSegments[0])
		pattern.EdgeType = strings.TrimSpace(edgeSegments[1])
	} else {
		pattern.EdgeVar = strings.TrimSpace(edgeSegments[0])
	}
	return pattern, nil
}

func parseWhereClauses(text string) ([]*whereClause, error) {
	parts := splitTopLevelKeyword(text, " AND ")
	clauses := make([]*whereClause, 0, len(parts))
	for _, part := range parts {
		if part == "" {
			return nil, fmt.Errorf("unsupported WHERE clause %q", text)
		}
		clause, err := parseWhereClause(part)
		if err != nil {
			return nil, err
		}
		clauses = append(clauses, clause)
	}
	return clauses, nil
}

func parseWhereClause(text string) (*whereClause, error) {
	text = strings.TrimSpace(text)
	if strings.HasSuffix(text, " IS NOT NULL") {
		varName, property, err := parsePropertyAccess(strings.TrimSuffix(text, " IS NOT NULL"))
		if err != nil {
			return nil, err
		}
		return &whereClause{Kind: whereIsNotNull, Var: varName, Property: property}, nil
	}
	if strings.HasSuffix(text, " IS NULL") {
		varName, property, err := parsePropertyAccess(strings.TrimSuffix(text, " IS NULL"))
		if err != nil {
			return nil, err
		}
		return &whereClause{Kind: whereIsNull, Var: varName, Property: property}, nil
	}
	if left, right, ok := splitOperator(text, " <=> "); ok {
		varName, property, err := parsePropertyAccess(left)
		if err != nil {
			return nil, err
		}
		expr, err := parseValueExpr(right)
		if err != nil {
			return nil, err
		}
		return &whereClause{Kind: whereVector, Var: varName, Property: property, Expr: expr}, nil
	}
	if left, right, ok := splitOperator(text, " @@ "); ok {
		varName, property, err := parsePropertyAccess(left)
		if err != nil {
			return nil, err
		}
		expr, err := parseValueExpr(right)
		if err != nil {
			return nil, err
		}
		return &whereClause{Kind: whereFTS, Var: varName, Property: property, Expr: expr}, nil
	}
	if left, right, ok := splitOperator(text, " = "); ok {
		if varName, ok := parseBindingIDAccess(left); ok {
			expr, err := parseValueExpr(right)
			if err != nil {
				return nil, err
			}
			return &whereClause{Kind: whereBindingID, Var: varName, Expr: expr}, nil
		}
		varName, property, err := parsePropertyAccess(left)
		if err != nil {
			return nil, err
		}
		expr, err := parseValueExpr(right)
		if err != nil {
			return nil, err
		}
		return &whereClause{Kind: whereEquals, Var: varName, Property: property, Expr: expr}, nil
	}
	return nil, fmt.Errorf("unsupported WHERE clause %q", text)
}

func parseSetClause(text string) (*setClause, error) {
	if left, right, ok := splitOperator(text, " += "); ok {
		name := strings.TrimSpace(left)
		if name == "" || strings.Contains(name, ".") {
			return nil, fmt.Errorf("invalid SET merge target %q", left)
		}
		expr, err := parseValueExpr(right)
		if err != nil {
			return nil, err
		}
		return &setClause{Kind: setMerge, Var: name, Expr: expr}, nil
	}

	left, right, ok := splitOperator(text, " = ")
	if !ok {
		return nil, fmt.Errorf("unsupported SET clause %q", text)
	}
	expr, err := parseValueExpr(right)
	if err != nil {
		return nil, err
	}
	if varName, property, err := parsePropertyAccess(left); err == nil {
		return &setClause{Kind: setProperty, Var: varName, Property: property, Expr: expr}, nil
	}
	name := strings.TrimSpace(left)
	if name == "" {
		return nil, fmt.Errorf("invalid SET target %q", left)
	}
	return &setClause{Kind: setReplace, Var: name, Expr: expr}, nil
}

func parseCreateClause(text string) (*createClause, error) {
	leftEnd := strings.Index(text, ")-[")
	if leftEnd < 0 {
		return nil, fmt.Errorf("unsupported CREATE clause %q", text)
	}
	rightStart := strings.Index(text[leftEnd+3:], "]->")
	if rightStart < 0 {
		return nil, fmt.Errorf("unsupported CREATE clause %q", text)
	}
	rightStart += leftEnd + 3

	sourceBody, err := trimEnclosed(text[:leftEnd+1], '(', ')')
	if err != nil {
		return nil, err
	}
	targetBody, err := trimEnclosed(text[rightStart+3:], '(', ')')
	if err != nil {
		return nil, err
	}

	edgeBody := strings.TrimSpace(text[leftEnd+3 : rightStart])
	propStart := findTopLevelRune(edgeBody, '{')
	props := map[string]valueExpr{}
	edgePrefix := edgeBody
	if propStart >= 0 {
		propEnd := findMatchingBrace(edgeBody, propStart, '{', '}')
		if propEnd < 0 {
			return nil, fmt.Errorf("unterminated CREATE property map in %q", text)
		}
		parsedProps, err := parsePropertyExprMap(edgeBody[propStart+1 : propEnd])
		if err != nil {
			return nil, err
		}
		props = parsedProps
		edgePrefix = strings.TrimSpace(edgeBody[:propStart])
	}

	edgeSegments := strings.SplitN(edgePrefix, ":", 2)
	if len(edgeSegments) != 2 {
		return nil, fmt.Errorf("invalid CREATE edge pattern %q", text)
	}

	return &createClause{
		SourceVar: strings.TrimSpace(sourceBody),
		TargetVar: strings.TrimSpace(targetBody),
		EdgeType:  strings.TrimSpace(edgeSegments[1]),
		Props:     props,
	}, nil
}

func parseDeleteClause(text string) (*deleteClause, error) {
	parts := splitTopLevel(text, ',')
	vars := make([]string, 0, len(parts))
	for _, part := range parts {
		name := strings.TrimSpace(part)
		if name == "" {
			return nil, fmt.Errorf("invalid DELETE clause %q", text)
		}
		vars = append(vars, name)
	}
	if len(vars) == 0 {
		return nil, fmt.Errorf("invalid DELETE clause %q", text)
	}
	return &deleteClause{Vars: vars}, nil
}

func parseRemoveClause(text string) (*removeClause, error) {
	parts := splitTopLevel(text, ',')
	items := make([]removeItem, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			return nil, fmt.Errorf("invalid REMOVE clause %q", text)
		}
		if varName, property, err := parsePropertyAccess(part); err == nil {
			items = append(items, removeItem{Var: varName, Property: property})
			continue
		}
		left, right, ok := splitOperator(part, ":")
		if !ok {
			return nil, fmt.Errorf("invalid REMOVE clause item %q", part)
		}
		varName := strings.TrimSpace(left)
		label := strings.TrimSpace(right)
		if varName == "" || label == "" {
			return nil, fmt.Errorf("invalid REMOVE clause item %q", part)
		}
		items = append(items, removeItem{Var: varName, Label: label})
	}
	return &removeClause{Items: items}, nil
}

func parseReturnClause(text string) (*returnClause, error) {
	text = strings.TrimSpace(text)
	if strings.HasPrefix(text, "count(") {
		closeIdx := strings.Index(text, ")")
		if closeIdx < 0 {
			return nil, fmt.Errorf("invalid count return %q", text)
		}
		derivedAlias := strings.TrimSpace(text[:closeIdx+1])
		countVar := strings.TrimSpace(text[len("count("):closeIdx])
		rest := strings.TrimSpace(text[closeIdx+1:])
		switch {
		case rest == "":
			return &returnClause{
				CountVar:   countVar,
				CountAlias: derivedAlias,
			}, nil
		case !strings.HasPrefix(rest, "AS "):
			return nil, fmt.Errorf("invalid count return %q", text)
		}
		return &returnClause{
			CountVar:   countVar,
			CountAlias: strings.TrimSpace(strings.TrimPrefix(rest, "AS ")),
		}, nil
	}

	parts := splitTopLevel(text, ',')
	projections := make([]projection, 0, len(parts))
	for _, part := range parts {
		exprText := strings.TrimSpace(part)
		alias := exprText
		pieces := strings.SplitN(exprText, " AS ", 2)
		if len(pieces) == 2 {
			exprText = strings.TrimSpace(pieces[0])
			alias = strings.TrimSpace(pieces[1])
		}
		if varName, ok := parseBindingIDAccess(exprText); ok {
			projections = append(projections, projection{
				Kind:  projectionBindingID,
				Var:   varName,
				Alias: alias,
			})
			continue
		}
		if !strings.Contains(exprText, ".") {
			projections = append(projections, projection{
				Kind:  projectionValue,
				Var:   exprText,
				Alias: alias,
			})
			continue
		}
		varName, property, err := parsePropertyAccess(exprText)
		if err != nil {
			return nil, err
		}
		projections = append(projections, projection{
			Kind:     projectionProperty,
			Var:      varName,
			Property: property,
			Alias:    alias,
		})
	}
	return &returnClause{Projections: projections}, nil
}

func (pattern nodePattern) apply(tx *Tx, rows []queryRow) ([]queryRow, error) {
	nextRows := make([]queryRow, 0)
	for _, row := range rows {
		for _, nodeID := range store.SortedNodeIDs(tx.graph) {
			node := tx.graph.Nodes[nodeID]
			if !store.LabelsMatch(node, pattern.Labels) {
				continue
			}
			if !store.PropertiesMatch(node.Properties, pattern.Properties) {
				continue
			}
			if pattern.Var != "" {
				if existing, ok := row.Bindings[pattern.Var]; ok {
					if existing.Node == nil || existing.Node.ID != node.ID {
						continue
					}
					nextRows = append(nextRows, row)
					continue
				}
			}
			nextRow := row.clone()
			if pattern.Var != "" {
				nextRow.Bindings[pattern.Var] = boundValue{Node: node}
			}
			nextRows = append(nextRows, nextRow)
		}
	}
	return nextRows, nil
}

func (pattern edgePattern) apply(tx *Tx, rows []queryRow) ([]queryRow, error) {
	nextRows := make([]queryRow, 0)
	for _, row := range rows {
		for _, edgeID := range store.SortedEdgeIDs(tx.graph) {
			edge := tx.graph.Edges[edgeID]
			if pattern.EdgeType != "" && edge.Type != pattern.EdgeType {
				continue
			}
			source := tx.graph.Nodes[edge.SourceID]
			target := tx.graph.Nodes[edge.TargetID]
			if source == nil || target == nil {
				continue
			}
			if !store.LabelsMatch(source, pattern.Left.Labels) || !store.PropertiesMatch(source.Properties, pattern.Left.Properties) {
				continue
			}
			if !store.LabelsMatch(target, pattern.Right.Labels) || !store.PropertiesMatch(target.Properties, pattern.Right.Properties) {
				continue
			}
			if !bindingMatchesNode(row, pattern.Left.Var, source) || !bindingMatchesNode(row, pattern.Right.Var, target) {
				continue
			}
			if pattern.EdgeVar != "" {
				if existing, ok := row.Bindings[pattern.EdgeVar]; ok && (existing.Edge == nil || existing.Edge.ID != edge.ID) {
					continue
				}
			}

			nextRow := row.clone()
			if pattern.Left.Var != "" {
				nextRow.Bindings[pattern.Left.Var] = boundValue{Node: source}
			}
			if pattern.Right.Var != "" {
				nextRow.Bindings[pattern.Right.Var] = boundValue{Node: target}
			}
			if pattern.EdgeVar != "" {
				nextRow.Bindings[pattern.EdgeVar] = boundValue{Edge: edge}
			}
			nextRows = append(nextRows, nextRow)
		}
	}
	return nextRows, nil
}

func (clause *whereClause) apply(rows []queryRow, params map[string]any) ([]queryRow, error) {
	filtered := make([]queryRow, 0, len(rows))
	for _, row := range rows {
		binding, ok := row.Bindings[clause.Var]
		if !ok {
			continue
		}
		value, exists := propertyFromBinding(binding, clause.Property)
		switch clause.Kind {
		case whereEquals:
			expected, err := clause.Expr.eval(row, params)
			if err != nil {
				return nil, err
			}
			if exists && reflect.DeepEqual(value, expected) {
				filtered = append(filtered, row)
			}
		case whereIsNull:
			if !exists || value == nil {
				filtered = append(filtered, row)
			}
		case whereIsNotNull:
			if exists && value != nil {
				filtered = append(filtered, row)
			}
		case whereVector:
			if !exists {
				continue
			}
			vector, ok := value.([]float32)
			if !ok {
				continue
			}
			expected, err := clause.Expr.eval(row, params)
			if err != nil {
				return nil, err
			}
			queryVector, ok := expected.([]float32)
			if !ok {
				return nil, fmt.Errorf("vector comparison requires []float32, got %T", expected)
			}
			distance, err := search.VectorDistance(vector, queryVector)
			if err != nil {
				return nil, err
			}
			nextRow := row.clone()
			nextRow.Order = float64(distance)
			filtered = append(filtered, nextRow)
		case whereFTS:
			if !exists {
				continue
			}
			text, ok := value.(string)
			if !ok {
				continue
			}
			expected, err := clause.Expr.eval(row, params)
			if err != nil {
				return nil, err
			}
			queryText, ok := expected.(string)
			if !ok {
				return nil, fmt.Errorf("fts comparison requires string, got %T", expected)
			}
			score := search.FTSScore(text, search.Tokenize(queryText))
			if score <= 0 {
				continue
			}
			nextRow := row.clone()
			nextRow.Order = -float64(score)
			filtered = append(filtered, nextRow)
		case whereBindingID:
			expected, err := clause.Expr.eval(row, params)
			if err != nil {
				return nil, err
			}
			expectedID, ok := normalizeInt64(expected)
			if !ok {
				return nil, fmt.Errorf("id comparison requires integer, got %T", expected)
			}
			gotID, ok := bindingID(binding)
			if ok && gotID == expectedID {
				filtered = append(filtered, row)
			}
		default:
			return nil, fmt.Errorf("unsupported WHERE kind %q", clause.Kind)
		}
	}

	if clause.Kind == whereVector || clause.Kind == whereFTS {
		slices.SortFunc(filtered, func(a queryRow, b queryRow) int {
			if a.Order < b.Order {
				return -1
			}
			if a.Order > b.Order {
				return 1
			}
			return compareRowBindings(a, b)
		})
	}
	return filtered, nil
}

func (clause *setClause) apply(rows []queryRow, params map[string]any) error {
	for _, row := range rows {
		binding, ok := row.Bindings[clause.Var]
		if !ok {
			continue
		}
		value, err := clause.Expr.eval(row, params)
		if err != nil {
			return err
		}
		normalized, err := store.NormalizeValue(value)
		if err != nil {
			return err
		}
		switch clause.Kind {
		case setProperty:
			switch {
			case binding.Node != nil:
				if normalized == nil {
					delete(binding.Node.Properties, clause.Property)
				} else {
					binding.Node.Properties[clause.Property] = normalized
				}
			case binding.Edge != nil:
				if normalized == nil {
					delete(binding.Edge.Properties, clause.Property)
				} else {
					binding.Edge.Properties[clause.Property] = normalized
				}
			default:
				return fmt.Errorf("binding %q is neither node nor edge", clause.Var)
			}
		case setReplace:
			props, err := replacementPropertyMap(normalized)
			if err != nil {
				return err
			}
			switch {
			case binding.Node != nil:
				binding.Node.Properties = props
			case binding.Edge != nil:
				binding.Edge.Properties = props
			default:
				return fmt.Errorf("binding %q is neither node nor edge", clause.Var)
			}
		case setMerge:
			props, err := mergePropertyMap(normalized)
			if err != nil {
				return err
			}
			switch {
			case binding.Node != nil:
				mergeMutationProperties(binding.Node.Properties, props)
			case binding.Edge != nil:
				mergeMutationProperties(binding.Edge.Properties, props)
			default:
				return fmt.Errorf("binding %q is neither node nor edge", clause.Var)
			}
		default:
			return fmt.Errorf("unsupported SET kind %q", clause.Kind)
		}
	}
	return nil
}

func (clause *removeClause) apply(rows []queryRow) error {
	for _, row := range rows {
		for _, item := range clause.Items {
			binding, ok := row.Bindings[item.Var]
			if !ok {
				continue
			}
			switch {
			case item.Property != "":
				switch {
				case binding.Node != nil:
					delete(binding.Node.Properties, item.Property)
				case binding.Edge != nil:
					delete(binding.Edge.Properties, item.Property)
				default:
					return fmt.Errorf("binding %q is neither node nor edge", item.Var)
				}
			case item.Label != "":
				if binding.Node == nil {
					return fmt.Errorf("binding %q is not a node", item.Var)
				}
				binding.Node.Labels = removeLabel(binding.Node.Labels, item.Label)
			default:
				return fmt.Errorf("invalid REMOVE item for binding %q", item.Var)
			}
		}
	}
	return nil
}

func (clause *createClause) apply(tx *Tx, rows []queryRow, params map[string]any) error {
	for _, row := range rows {
		sourceBinding, ok := row.Bindings[clause.SourceVar]
		if !ok || sourceBinding.Node == nil {
			return fmt.Errorf("unknown source binding %q", clause.SourceVar)
		}
		targetBinding, ok := row.Bindings[clause.TargetVar]
		if !ok || targetBinding.Node == nil {
			return fmt.Errorf("unknown target binding %q", clause.TargetVar)
		}

		props := make(map[string]any, len(clause.Props))
		for key, expr := range clause.Props {
			value, err := expr.eval(row, params)
			if err != nil {
				return err
			}
			normalized, err := store.NormalizeValue(value)
			if err != nil {
				return err
			}
			props[key] = normalized
		}

		if _, err := tx.CreateEdge(sourceBinding.Node.ID, targetBinding.Node.ID, clause.EdgeType, CreateEdgeOptions{
			Properties: props,
		}); err != nil {
			return err
		}
	}
	return nil
}

func (clause *deleteClause) apply(tx *Tx, rows []queryRow) error {
	nodeIDs := map[uint64]struct{}{}
	edgeIDs := map[uint64]struct{}{}

	for _, row := range rows {
		for _, name := range clause.Vars {
			binding, ok := row.Bindings[name]
			if !ok {
				return fmt.Errorf("unknown binding %q", name)
			}
			switch {
			case binding.Edge != nil:
				edgeIDs[binding.Edge.ID] = struct{}{}
			case binding.Node != nil:
				nodeIDs[binding.Node.ID] = struct{}{}
			default:
				return fmt.Errorf("binding %q is neither node nor edge", name)
			}
		}
	}

	for edgeID := range edgeIDs {
		delete(tx.graph.Edges, edgeID)
	}
	for nodeID := range nodeIDs {
		if err := tx.DeleteNode(nodeID); err != nil {
			return err
		}
	}
	return nil
}

func (clause *returnClause) render(rows []queryRow) (QueryResult, error) {
	if clause.CountAlias != "" {
		return QueryResult{
			Columns: []string{clause.CountAlias},
			Rows: []map[string]any{
				{clause.CountAlias: int64(len(rows))},
			},
		}, nil
	}

	result := QueryResult{Columns: make([]string, 0, len(clause.Projections))}
	for _, projection := range clause.Projections {
		result.Columns = append(result.Columns, projection.Alias)
	}

	for _, row := range rows {
		resultRow := make(map[string]any, len(clause.Projections))
		for _, projection := range clause.Projections {
			switch projection.Kind {
			case projectionBindingID:
				binding, ok := row.Bindings[projection.Var]
				if !ok {
					resultRow[projection.Alias] = nil
					continue
				}
				value, ok := bindingID(binding)
				if !ok {
					resultRow[projection.Alias] = nil
					continue
				}
				resultRow[projection.Alias] = value
			case projectionProperty:
				binding, ok := row.Bindings[projection.Var]
				if !ok {
					resultRow[projection.Alias] = nil
					continue
				}
				value, exists := propertyFromBinding(binding, projection.Property)
				if !exists {
					resultRow[projection.Alias] = nil
					continue
				}
				resultRow[projection.Alias] = store.CloneValue(value)
			case projectionValue:
				binding, ok := row.Bindings[projection.Var]
				if !ok {
					resultRow[projection.Alias] = nil
					continue
				}
				if !binding.HasValue {
					return QueryResult{}, fmt.Errorf("unsupported value projection for binding %q", projection.Var)
				}
				resultRow[projection.Alias] = store.CloneValue(binding.Value)
			default:
				return QueryResult{}, fmt.Errorf("unsupported projection kind %q", projection.Kind)
			}
		}
		result.Rows = append(result.Rows, resultRow)
	}
	return result, nil
}

func (expr literalExpr) eval(_ queryRow, _ map[string]any) (any, error) {
	return store.CloneValue(expr.Value), nil
}

func (expr mapLiteralExpr) eval(row queryRow, params map[string]any) (any, error) {
	out := make(map[string]any, len(expr.Entries))
	for key, item := range expr.Entries {
		value, err := item.eval(row, params)
		if err != nil {
			return nil, err
		}
		normalized, err := store.NormalizeValue(value)
		if err != nil {
			return nil, err
		}
		out[key] = normalized
	}
	return out, nil
}

func (expr paramExpr) eval(_ queryRow, params map[string]any) (any, error) {
	value, ok := params[expr.Name]
	if !ok {
		return nil, fmt.Errorf("missing query parameter %q", expr.Name)
	}
	return store.NormalizeValue(value)
}

func (expr variableExpr) eval(row queryRow, _ map[string]any) (any, error) {
	value, ok := row.Bindings[expr.Name]
	if !ok {
		return nil, fmt.Errorf("unknown binding %q", expr.Name)
	}
	if value.HasValue {
		return store.CloneValue(value.Value), nil
	}
	return value, nil
}

func parseValueExpr(text string) (valueExpr, error) {
	text = strings.TrimSpace(text)
	switch {
	case text == "null":
		return literalExpr{Value: nil}, nil
	case text == "true":
		return literalExpr{Value: true}, nil
	case text == "false":
		return literalExpr{Value: false}, nil
	case strings.HasPrefix(text, "$"):
		return paramExpr{Name: strings.TrimPrefix(text, "$")}, nil
	case strings.HasPrefix(text, "{") && strings.HasSuffix(text, "}"):
		entries, err := parsePropertyExprMap(strings.TrimSpace(text[1 : len(text)-1]))
		if err != nil {
			return nil, err
		}
		return mapLiteralExpr{Entries: entries}, nil
	case strings.HasPrefix(text, "\""):
		unquoted, err := strconv.Unquote(text)
		if err != nil {
			return nil, err
		}
		return literalExpr{Value: unquoted}, nil
	default:
		if i, err := strconv.ParseInt(text, 10, 64); err == nil {
			return literalExpr{Value: i}, nil
		}
		if f, err := strconv.ParseFloat(text, 64); err == nil {
			return literalExpr{Value: f}, nil
		}
		if strings.Contains(text, ".") {
			return nil, fmt.Errorf("unsupported expression %q", text)
		}
		return variableExpr{Name: text}, nil
	}
}

func parsePropertyLiteralMap(text string) (map[string]any, error) {
	out := make(map[string]any)
	if strings.TrimSpace(text) == "" {
		return out, nil
	}
	for _, part := range splitTopLevel(text, ',') {
		key, rawValue, err := splitPropertyAssignment(part)
		if err != nil {
			return nil, err
		}
		expr, err := parseValueExpr(rawValue)
		if err != nil {
			return nil, err
		}
		literal, ok := expr.(literalExpr)
		if !ok {
			return nil, fmt.Errorf("MATCH properties require literal values, got %q", rawValue)
		}
		if _, exists := out[key]; exists {
			return nil, fmt.Errorf("duplicate map key %q", key)
		}
		out[key] = literal.Value
	}
	return out, nil
}

func parsePropertyExprMap(text string) (map[string]valueExpr, error) {
	out := make(map[string]valueExpr)
	if strings.TrimSpace(text) == "" {
		return out, nil
	}
	for _, part := range splitTopLevel(text, ',') {
		key, rawValue, err := splitPropertyAssignment(part)
		if err != nil {
			return nil, err
		}
		expr, err := parseValueExpr(rawValue)
		if err != nil {
			return nil, err
		}
		if _, exists := out[key]; exists {
			return nil, fmt.Errorf("duplicate map key %q", key)
		}
		out[key] = expr
	}
	return out, nil
}

func splitPropertyAssignment(text string) (string, string, error) {
	key, value, ok := splitOperator(text, ":")
	if !ok {
		return "", "", fmt.Errorf("invalid property assignment %q", text)
	}
	key = strings.TrimSpace(key)
	if key == "" {
		return "", "", errors.New("property key must be non-empty")
	}
	return key, strings.TrimSpace(value), nil
}

func parsePropertyAccess(text string) (string, string, error) {
	parts := strings.SplitN(strings.TrimSpace(text), ".", 2)
	if len(parts) != 2 {
		return "", "", fmt.Errorf("invalid property access %q", text)
	}
	return strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1]), nil
}

func parseBindingIDAccess(text string) (string, bool) {
	text = strings.TrimSpace(text)
	if !strings.HasPrefix(text, "id(") || !strings.HasSuffix(text, ")") {
		return "", false
	}
	name := strings.TrimSpace(text[len("id(") : len(text)-1])
	if name == "" {
		return "", false
	}
	return name, true
}

func splitOperator(text string, operator string) (string, string, bool) {
	index := strings.Index(text, operator)
	if index < 0 {
		return "", "", false
	}
	return strings.TrimSpace(text[:index]), strings.TrimSpace(text[index+len(operator):]), true
}

func splitOnNextClause(input string, keywords ...string) (string, string, string) {
	inString := false
	braceDepth := 0
	bracketDepth := 0
	parenDepth := 0
	for i := 0; i < len(input); i++ {
		switch input[i] {
		case '"':
			if i == 0 || input[i-1] != '\\' {
				inString = !inString
			}
		case '{':
			if !inString {
				braceDepth++
			}
		case '}':
			if !inString && braceDepth > 0 {
				braceDepth--
			}
		case '[':
			if !inString {
				bracketDepth++
			}
		case ']':
			if !inString && bracketDepth > 0 {
				bracketDepth--
			}
		case '(':
			if !inString {
				parenDepth++
			}
		case ')':
			if !inString && parenDepth > 0 {
				parenDepth--
			}
		}
		if inString || braceDepth != 0 || bracketDepth != 0 || parenDepth != 0 {
			continue
		}
		for _, keyword := range keywords {
			if strings.HasPrefix(input[i:], keyword) {
				return strings.TrimSpace(input[:i]), keyword, strings.TrimSpace(input[i+len(keyword):])
			}
		}
	}
	return strings.TrimSpace(input), "", ""
}

func splitLimitClause(text string) (string, string, bool) {
	head, keyword, tail := splitOnNextClause(text, " LIMIT ")
	if keyword == "" {
		return text, "", false
	}
	return head, tail, true
}

func trimEnclosed(text string, open byte, close byte) (string, error) {
	text = strings.TrimSpace(text)
	if len(text) < 2 || text[0] != open || text[len(text)-1] != close {
		return "", fmt.Errorf("expected %c...%c, got %q", open, close, text)
	}
	return strings.TrimSpace(text[1 : len(text)-1]), nil
}

func splitTopLevel(text string, separator rune) []string {
	parts := make([]string, 0)
	start := 0
	inString := false
	braceDepth := 0
	bracketDepth := 0
	parenDepth := 0

	for i, r := range text {
		switch r {
		case '"':
			if i == 0 || text[i-1] != '\\' {
				inString = !inString
			}
		case '{':
			if !inString {
				braceDepth++
			}
		case '}':
			if !inString && braceDepth > 0 {
				braceDepth--
			}
		case '[':
			if !inString {
				bracketDepth++
			}
		case ']':
			if !inString && bracketDepth > 0 {
				bracketDepth--
			}
		case '(':
			if !inString {
				parenDepth++
			}
		case ')':
			if !inString && parenDepth > 0 {
				parenDepth--
			}
		}

		if r == separator && !inString && braceDepth == 0 && bracketDepth == 0 && parenDepth == 0 {
			parts = append(parts, strings.TrimSpace(text[start:i]))
			start = i + 1
		}
	}
	parts = append(parts, strings.TrimSpace(text[start:]))
	return parts
}

func splitTopLevelKeyword(text string, keyword string) []string {
	parts := make([]string, 0)
	start := 0
	inString := false
	braceDepth := 0
	bracketDepth := 0
	parenDepth := 0

	for i := 0; i < len(text); i++ {
		switch text[i] {
		case '"':
			if i == 0 || text[i-1] != '\\' {
				inString = !inString
			}
		case '{':
			if !inString {
				braceDepth++
			}
		case '}':
			if !inString && braceDepth > 0 {
				braceDepth--
			}
		case '[':
			if !inString {
				bracketDepth++
			}
		case ']':
			if !inString && bracketDepth > 0 {
				bracketDepth--
			}
		case '(':
			if !inString {
				parenDepth++
			}
		case ')':
			if !inString && parenDepth > 0 {
				parenDepth--
			}
		}

		if inString || braceDepth != 0 || bracketDepth != 0 || parenDepth != 0 {
			continue
		}
		if strings.HasPrefix(text[i:], keyword) {
			parts = append(parts, strings.TrimSpace(text[start:i]))
			start = i + len(keyword)
			i += len(keyword) - 1
		}
	}

	parts = append(parts, strings.TrimSpace(text[start:]))
	return parts
}

func findTopLevelRune(text string, target rune) int {
	inString := false
	braceDepth := 0
	bracketDepth := 0
	parenDepth := 0
	for i, r := range text {
		if !inString && braceDepth == 0 && bracketDepth == 0 && parenDepth == 0 && r == target {
			return i
		}
		switch r {
		case '"':
			if i == 0 || text[i-1] != '\\' {
				inString = !inString
			}
		case '{':
			if !inString {
				braceDepth++
			}
		case '}':
			if !inString && braceDepth > 0 {
				braceDepth--
			}
		case '[':
			if !inString {
				bracketDepth++
			}
		case ']':
			if !inString && bracketDepth > 0 {
				bracketDepth--
			}
		case '(':
			if !inString {
				parenDepth++
			}
		case ')':
			if !inString && parenDepth > 0 {
				parenDepth--
			}
		}
	}
	return -1
}

func findMatchingBrace(text string, start int, open byte, close byte) int {
	depth := 0
	inString := false
	for i := start; i < len(text); i++ {
		switch text[i] {
		case '"':
			if i == 0 || text[i-1] != '\\' {
				inString = !inString
			}
		case open:
			if !inString {
				depth++
			}
		case close:
			if !inString {
				depth--
				if depth == 0 {
					return i
				}
			}
		}
	}
	return -1
}

func bindingMatchesNode(row queryRow, name string, node *store.NodeRecord) bool {
	if name == "" {
		return true
	}
	binding, ok := row.Bindings[name]
	if !ok {
		return true
	}
	return binding.Node != nil && binding.Node.ID == node.ID
}

func propertyFromBinding(binding boundValue, property string) (any, bool) {
	switch {
	case binding.Node != nil:
		value, ok := binding.Node.Properties[property]
		return value, ok
	case binding.Edge != nil:
		value, ok := binding.Edge.Properties[property]
		return value, ok
	case binding.HasValue:
		value, ok := binding.Value.(map[string]any)
		if !ok {
			return nil, false
		}
		prop, ok := value[property]
		return prop, ok
	default:
		return nil, false
	}
}

func (row queryRow) clone() queryRow {
	bindings := make(map[string]boundValue, len(row.Bindings))
	for key, value := range row.Bindings {
		bindings[key] = value
	}
	return queryRow{Bindings: bindings, Order: row.Order}
}

func compareRowBindings(left queryRow, right queryRow) int {
	leftID := lowestBindingID(left.Bindings)
	rightID := lowestBindingID(right.Bindings)
	switch {
	case leftID < rightID:
		return -1
	case leftID > rightID:
		return 1
	default:
		return 0
	}
}

func lowestBindingID(bindings map[string]boundValue) uint64 {
	var lowest uint64
	for _, binding := range bindings {
		var candidate uint64
		switch {
		case binding.Node != nil:
			candidate = binding.Node.ID
		case binding.Edge != nil:
			candidate = binding.Edge.ID
		default:
			continue
		}
		if lowest == 0 || candidate < lowest {
			lowest = candidate
		}
	}
	return lowest
}

func bindingID(binding boundValue) (int64, bool) {
	switch {
	case binding.Node != nil:
		return int64(binding.Node.ID), true
	case binding.Edge != nil:
		return int64(binding.Edge.ID), true
	default:
		return 0, false
	}
}

func normalizeInt64(value any) (int64, bool) {
	switch v := value.(type) {
	case int64:
		return v, true
	case int:
		return int64(v), true
	default:
		return 0, false
	}
}

func replacementPropertyMap(value any) (map[string]any, error) {
	props, ok := value.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("property-map mutation requires map value, got %T", value)
	}
	out := make(map[string]any, len(props))
	for key, item := range props {
		if item == nil {
			continue
		}
		out[key] = item
	}
	return out, nil
}

func mergePropertyMap(value any) (map[string]any, error) {
	props, ok := value.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("property-map mutation requires map value, got %T", value)
	}
	return props, nil
}

func mergeMutationProperties(dst map[string]any, src map[string]any) {
	for key, value := range src {
		if value == nil {
			delete(dst, key)
			continue
		}
		dst[key] = value
	}
}

func removeLabel(labels []string, target string) []string {
	if len(labels) == 0 {
		return nil
	}
	out := make([]string, 0, len(labels))
	for _, label := range labels {
		if label != target {
			out = append(out, label)
		}
	}
	return out
}
