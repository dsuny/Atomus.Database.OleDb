using System;
using System.Data.Common;
using System.Data.OleDb;

namespace Atomus.Database
{
    public class OleDb : IDatabase
    {
        OleDbDataAdapter sqlDataAdapter;

        public OleDb()
        {
            this.sqlDataAdapter = new OleDbDataAdapter
            {
                SelectCommand = new OleDbCommand
                {
                    Connection = new OleDbConnection()
                }
            };
        }

        DbParameter IDatabase.AddParameter(string parameterName, DbType dbType, int size)
        {
            OleDbCommand sqlCommand;

            try
            {
                sqlCommand = this.sqlDataAdapter.SelectCommand;

                if (size == 0)
                    return sqlCommand.Parameters.Add(parameterName, this.DbTypeConvert(dbType));
                else
                    return sqlCommand.Parameters.Add(parameterName, this.DbTypeConvert(dbType), size);
            }
            catch (AtomusException exception)
            {
                throw exception;
            }
            catch (Exception exception)
            {
                throw new AtomusException(exception);
            }
        }

        OleDbType DbTypeConvert(DbType dbType)
        {
            switch (dbType)
            {
                case DbType.BigInt:
                    return OleDbType.BigInt;

                case DbType.Binary:
                    return OleDbType.Binary;

                case DbType.Bit:
                    throw new AtomusException("DbType.Bit type Not Support.");

                case DbType.Char:
                    return OleDbType.Char;

                case DbType.Date:
                    return OleDbType.Date;

                case DbType.DateTime:
                    return OleDbType.DBTimeStamp;

                case DbType.DateTime2:
                    throw new AtomusException("DbType.DateTime2 type Not Support.");

                case DbType.DateTimeOffset:
                    throw new AtomusException("DbType.DateTimeOffset type Not Support.");

                case DbType.Decimal:
                    return OleDbType.Decimal;

                case DbType.Float:
                    throw new AtomusException("DbType.Float type Not Support.");

                case DbType.Image:
                    throw new AtomusException("DbType.Image type Not Support.");

                case DbType.Int:
                    return OleDbType.Integer;

                case DbType.Money:
                    return OleDbType.Currency;

                case DbType.NChar:
                    throw new AtomusException("DbType.NChar type Not Support.");

                case DbType.NText:
                    throw new AtomusException("DbType.NText type Not Support.");

                case DbType.NVarChar:
                    throw new AtomusException("DbType.NVarChar type Not Support.");

                case DbType.Real:
                    throw new AtomusException("DbType.Real type Not Support.");

                case DbType.SmallDateTime:
                    throw new AtomusException("DbType.SmallDateTime type Not Support.");

                case DbType.SmallInt:
                    return OleDbType.SmallInt;

                case DbType.SmallMoney:
                    throw new AtomusException("DbType.SmallMoney type Not Support.");

                case DbType.Structured:
                    throw new AtomusException("DbType.Structured type Not Support.");

                case DbType.Text:
                    throw new AtomusException("DbType.Structured type Not Support.");

                case DbType.Time:
                    return OleDbType.DBTime;

                case DbType.Timestamp:
                    return OleDbType.DBTimeStamp;

                case DbType.TinyInt:
                    return OleDbType.TinyInt;

                case DbType.Udt:
                    throw new AtomusException("DbType.Udt type Not Support.");

                case DbType.UniqueIdentifier:
                    return OleDbType.Guid;

                case DbType.VarBinary:
                    return OleDbType.VarBinary;

                case DbType.VarChar:
                    return OleDbType.VarChar;

                case DbType.Variant:
                    return OleDbType.Variant;

                case DbType.Xml:
                    throw new AtomusException("DbType.Xml type Not Support.");

                default:
                    return OleDbType.Variant;
            }
        }

        DbCommand IDatabase.Command
        {
            get
            {
                return this.sqlDataAdapter.SelectCommand;
            }
        }

        DbConnection IDatabase.Connection
        {
            get
            {
                return this.sqlDataAdapter.SelectCommand.Connection;
            }
        }

        DbDataAdapter IDatabase.DataAdapter
        {
            get
            {
                return this.sqlDataAdapter;
            }
        }

        DbTransaction IDatabase.Transaction
        {
            get
            {
                return this.sqlDataAdapter.SelectCommand.Transaction;
            }
        }

        void IDatabase.DeriveParameters()
        {
            try
            {
                OleDbCommandBuilder.DeriveParameters(this.sqlDataAdapter.SelectCommand);
            }
            catch (AtomusException exception)
            {
                throw exception;
            }
            catch (Exception exception)
            {
                throw new AtomusException(exception);
            }
        }

        void IDatabase.Close()
        {
            try
            {
                if (this.sqlDataAdapter.SelectCommand.Connection != null)
                {
                    this.sqlDataAdapter.SelectCommand.Connection.Close();
                    this.sqlDataAdapter.SelectCommand.Connection.Dispose();
                }

                if (this.sqlDataAdapter.SelectCommand != null)
                {
                    this.sqlDataAdapter.SelectCommand.Dispose();
                }

                if (this.sqlDataAdapter != null)
                {
                    this.sqlDataAdapter.Dispose();
                }
            }
            catch (AtomusException exception)
            {
                throw exception;
            }
            catch (Exception exception)
            {
                throw new AtomusException(exception);
            }
        }
    }
}