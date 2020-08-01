# timeseries-go
numeric time indexed dataframe tools in golang

## what it is
a lot of helper functions for time indexed numeric tables
all functions operate on interfaces

## interface requirements
```
TableGetter{
    Get(string)([]float64, error)
    GetIndex()([]time.Time, error)
    ListColumns() ([]string)
}
```

```
TableSetter{
    Set(string, []float64)(error)
    SetIndex([]time.Time)(error)
}
```

```
Table{
    TableSetter
    TableGetter
}
```