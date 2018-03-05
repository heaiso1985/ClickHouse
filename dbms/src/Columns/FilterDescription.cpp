#include <Columns/FilterDescription.h>

#include <Common/typeid_cast.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnConst.h>

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeNothing.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER;
}


ConstantFilterDescription::ConstantFilterDescription(const IColumn & column)
{
    if (column.onlyNull())
    {
        always_false = true;
        return;
    }

    if (column.isColumnConst())
    {
        if (static_cast<const ColumnConst &>(column).getValue<UInt8>())
            always_true = true;
        else
            always_false = true;
        return;
    }
}


FilterDescription::FilterDescription(const IColumn & column)
{
    if (const ColumnUInt8 * concrete_column = typeid_cast<const ColumnUInt8 *>(&column))
    {
        data = &concrete_column->getData();
        return;
    }

    if (const ColumnNullable * nullable_column = typeid_cast<const ColumnNullable *>(&column))
    {
        MutableColumnPtr mutable_holder = nullable_column->getNestedColumn().mutate();

        ColumnUInt8 * concrete_column = typeid_cast<ColumnUInt8 *>(mutable_holder.get());
        if (!concrete_column)
            throw Exception("Illegal type " + column.getName() + " of column for filter. Must be UInt8 or Nullable(UInt8).",
                ErrorCodes::ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER);

        const NullMap & null_map = nullable_column->getNullMapData();
        IColumn::Filter & res =  concrete_column->getData();

        size_t size = res.size();
        for (size_t i = 0; i < size; ++i)
            res[i] = res[i] && !null_map[i];

        data = &res;
        data_holder = std::move(mutable_holder);
        return;
    }

    throw Exception("Illegal type " + column.getName() + " of column for filter. Must be UInt8 or Nullable(UInt8) or Const variants of them.",
        ErrorCodes::ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER);
}

void FilterDescription::checkDataTypeForFilter(const IDataType & type)
{
    if (typeid_cast<const DataTypeUInt8 *>(&type))
        return;

    if (auto nullable_type = typeid_cast<const DataTypeNullable *>(&type))
    {
        if (typeid_cast<const DataTypeUInt8 *>(nullable_type->getNestedType().get())
            || typeid_cast<const DataTypeNothing *>(nullable_type->getNestedType().get()))
            return;
    }

    throw Exception("Illegal type " + type.getName() + " for filter expression. Must be UInt8 or Nullable(UInt8) or Nullable(Nothing)",
                    ErrorCodes::ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER);
}

}
