package test

import (
	"elasticsearch-data-import-go/es"
	"testing"
)

func TestAliasClient_FindIndexNameByAlias(t *testing.T) {
	alias := "user"
	indexes := es.Alias.FindIndexNameByAlias("user")
	t.Logf("FindIndexNameByAlias alias[%s] found indexes[%v]", alias, indexes)
}

func TestAliasClient_CreateAlias(t *testing.T) {
	alias := "user"
	index := "user_01"
	res := es.Alias.CreateAlias(alias, index)
	t.Logf("CreateAlias alias[%s] found indexes[%s] res[%t]", alias, index, res)
}

func TestAliasClient_DeleteAlias(t *testing.T) {
	alias := "user"
	index := "user_01"
	res := es.Alias.DeleteAlias(index, alias)
	t.Logf("DeleteAlias alias[%s] found indexes[%s] res[%t]", alias, index, res)
}
