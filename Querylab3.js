
use "sample_analytics"

//Ejercicio 1

db.accounts.find()

db.customers.find()

db.transactions.find()

//Ejercicio 2
//2.1

use("sample_analytics")

db.customers.aggregate([
  {
    $lookup: {
      from: "transactions",
      localField: "accounts",
      foreignField: "account_id",
      as: "Totaltransactions"
    }
  },
  {
    $unwind: "$Totaltransactions"
  },
  {
    $unwind: "$Totaltransactions.transactions"
  },
  {
    $set: {
      total: {
        $convert: {
          input: "$Totaltransactions.transactions.total",
          to: "decimal",
          onError: 0,
          onNull: 0
        }
      }
    }
  },
  {
    $group: {
      _id: "$_id",
      nombre: { $first: "$name" },
      ciudad: { $first: "$address" },
      transacciones: { $sum: 1 },
      monto: { $avg: "$total" }
    }
  },
  {
    $sort: { transacciones: -1 }
  }
])

//2.2
use("sample_analytics")

db.customers.aggregate([
  {
    $lookup: {
      from: "transactions",
      localField: "accounts",
      foreignField: "account_id",
      as: "transacciones"
    }
  },
  { $unwind: "$transacciones" },
  { $unwind: "$transacciones.transactions" },

  {
    $set: {
      total: {
        $convert: {
          input: "$transacciones.transactions.total",
          to: "double",
          onError: 0,
          onNull: 0
        }
      }
    }
  },

  {
    $set: {
      signedTotal: {
        $cond: [
          { $eq: ["$transacciones.transactions.transaction_code", "sell"] },
          "$total",
          { $multiply: ["$total", -1] }
        ]
      }
    }
  },

  {
    $group: {
      _id: "$_id",
      nombre: { $first: "$name" },
      Totalbalance: { $sum: "$signedTotal" }
    }
  },

  {
    $set: {
      categoria: {
        $switch: {
          branches: [
            { case: { $lt: ["$Totalbalance", 5000] }, then: "Bajo" },
            {
              case: {
                $and: [
                  { $gte: ["$Totalbalance", 5000] },
                  { $lte: ["$Totalbalance", 20000] }
                ]
              },
              then: "Medio"
            },
            { case: { $gt: ["$Totalbalance", 20000] }, then: "Alto" }
          ],
          default: "Bajo"
        }
      }
    }
  },

  {
    $project: {
      _id: 0,
      nombre: 1,
      Totalbalance: 1,
      categoria: 1
    }
  }
])

//2.3
use("sample_analytics")

db.customers.aggregate([
  {
    $lookup: {
      from: "transactions",
      localField: "accounts",
      foreignField: "account_id",
      as: "transacciones"
    }
  },
  { $unwind: "$transacciones" },
  { $unwind: "$transacciones.transactions" },

  {
    $set: {
      ciudad: {
        $let: {
          vars: {
            linea2: { $arrayElemAt: [{ $split: ["$address", "\n"] }, 1] }
          },
          in: {
            $trim: { input: { $arrayElemAt: [{ $split: ["$$linea2", ","] }, 0] } }
          }
        }
      }
    }
  },

  {
    $set: {
      total: {
        $convert: {
          input: "$transacciones.transactions.total",
          to: "double",
          onError: 0,
          onNull: 0
        }
      }
    }
  },

  {
    $set: {
      signedTotal: {
        $cond: [
          { $eq: ["$transacciones.transactions.transaction_code", "sell"] },
          "$total",
          { $multiply: ["$total", -1] }
        ]
      }
    }
  },

  {
    $group: {
      _id: "$_id",
      nombre: { $first: "$name" },
      ciudad: { $first: "$ciudad" },
      Totalbalance: { $sum: "$signedTotal" }
    }
  },

  { $sort: { ciudad: 1, Totalbalance: -1 } },
  {
    $group: {
      _id: "$ciudad",
      nombre: { $first: "$nombre" },
      Totalbalance: { $first: "$Totalbalance" }
    }
  },

  {
    $project: {
      _id: 0,
      ciudad: "$_id",
      nombre: 1,
      Totalbalance: 1
    }
  }
])


//2.4

use("sample_analytics")

db.transactions.aggregate([
  { $unwind: "$transactions" },

  {
    $match: {
      "transactions.date": {
        $gte: {
          $dateSubtract: {
            startDate: "$$NOW",
            unit: "month",
            amount: 6
          }
        }
      }
    }
  },

  {
    $set: {
      total: {
        $convert: {
          input: "$transactions.total",
          to: "double",
          onError: 0,
          onNull: 0
        }
      }
    }
  },

  { $sort: { total: -1 } },
  { $limit: 10 },

  {
    $lookup: {
      from: "customers",
      localField: "account_id",
      foreignField: "accounts",
      as: "cliente"
    }
  },

  { $unwind: "$cliente" },

  {
    $project: {
      _id: 0,
      nombre: "$cliente.name",
      fecha: "$transactions.date",
      tipo: "$transactions.transaction_code",
      simbolo: "$transactions.symbol",
      total: 1
    }
  }
])


// 2.5
use("sample_analytics")

db.customers.aggregate([
  {
    $lookup: {
      from: "transactions",
      localField: "accounts",
      foreignField: "account_id",
      as: "transacciones"
    }
  },
  { $unwind: "$transacciones" },
  { $unwind: "$transacciones.transactions" },

  {
    $set: {
      fecha: "$transacciones.transactions.date",
      total: {
        $convert: {
          input: "$transacciones.transactions.total",
          to: "double",
          onError: 0,
          onNull: 0
        }
      }
    }
  },

  {
    $group: {
      _id: "$_id",
      nombre: { $first: "$name" },
      num_transacciones: { $sum: 1 },

      total_antigua: {
        $bottom: {
          sortBy: { fecha: 1 },
          output: "$total"
        }
      },

      total_reciente: {
        $top: {
          sortBy: { fecha: 1 },
          output: "$total"
        }
      }
    }
  },

  { $match: { num_transacciones: { $gte: 2 } } },

  {
    $set: {
      variacion_porcentual: {
        $cond: [
          { $eq: ["$total_antigua", 0] },
          null,
          {
            $multiply: [
              {
                $divide: [
                  { $subtract: ["$total_reciente", "$total_antigua"] },
                  "$total_antigua"
                ]
              },
              100
            ]
          }
        ]
      }
    }
  },

  {
    $project: {
      _id: 0,
      nombre: 1,
      variacion_porcentual: 1
    }
  }
])


//2.6


use("sample_analytics")

db.transactions.aggregate([
  { $unwind: "$transactions" },

  {
    $set: {
      mes: { $dateToString: { format: "%Y-%m", date: "$transactions.date" } },
      tipo: "$transactions.transaction_code",
      total: {
        $convert: {
          input: "$transactions.total",
          to: "double",
          onError: 0,
          onNull: 0
        }
      }
    }
  },

  {
    $group: {
      _id: { mes: "$mes", tipo: "$tipo" },
      total_mes: { $sum: "$total" },
      promedio_mes: { $avg: "$total" },
      num_transacciones: { $sum: 1 }
    }
  },

  { $sort: { "_id.mes": 1, "_id.tipo": 1 } },

  {
    $project: {
      _id: 0,
      mes: "$_id.mes",
      tipo: "$_id.tipo",
      total_mes: 1,
      promedio_mes: 1,
      num_transacciones: 1
    }
  }
])


//2.7

use("sample_analytics")

db.customers.aggregate([
  {
    $lookup: {
      from: "transactions",
      localField: "accounts",
      foreignField: "account_id",
      as: "transacciones"
    }
  },

  {
    $match: {
      transacciones: { $eq: [] }
    }
  },

  {
    $project: {
      _id: 1,
      name: 1,
      email: 1,
      address: 1
    }
  },

  {
    $out: "inactive_customers"
  }
])


//2.8
use("sample_analytics")

db.accounts.aggregate([
  { $unwind: "$products" },
  {
    $group: {
      _id: "$products",
      total_cuentas: { $sum: 1 },
      balance_promedio: { $avg: "$limit" }
    }
  },
  {
    $project: {
      _id: 0,
      tipo_cuenta: "$_id",
      total_cuentas: 1,
      balance_promedio: 1
    }
  },
  { $out: "account_summaries" }
])


//2.9

use("sample_analytics")

db.customers.aggregate([
  {
    $lookup: {
      from: "transactions",
      localField: "accounts",
      foreignField: "account_id",
      as: "transacciones"
    }
  },
  { $unwind: "$transacciones" },
  { $unwind: "$transacciones.transactions" },

  {
    $set: {
      total: {
        $convert: {
          input: "$transacciones.transactions.total",
          to: "double",
          onError: 0,
          onNull: 0
        }
      }
    }
  },

  {
    $group: {
      _id: "$_id",
      nombre: { $first: "$name" },
      email: { $first: "$email" },
      total_balance: { $sum: "$total" },
      num_transacciones: { $sum: 1 }
    }
  },

  {
    $match: {
      total_balance: { $gt: 30000 },
      num_transacciones: { $gt: 5 }
    }
  },

  {
    $project: {
      _id: 0,
      nombre: 1,
      email: 1,
      total_balance: 1,
      num_transacciones: 1
    }
  },

  { $out: "high_value_customers" }
])

db.high_value_customers.find()


//2.10

use("sample_analytics")

db.customers.aggregate([
  {
    $lookup: {
      from: "transactions",
      localField: "accounts",
      foreignField: "account_id",
      as: "transacciones"
    }
  },

  { $unwind: "$transacciones" },
  { $unwind: "$transacciones.transactions" },

  {
    $set: {
      fecha: "$transacciones.transactions.date"
    }
  },

  {
    $match: {
      fecha: {
        $gte: new Date(new Date().setFullYear(new Date().getFullYear() - 1))
      }
    }
  },

  {
    $group: {
      _id: "$_id",
      nombre: { $first: "$name" },
      total_transacciones: { $sum: 1 }
    }
  },

  {
    $set: {
      promedio_mensual: { $divide: ["$total_transacciones", 12] }
    }
  },

  {
    $set: {
      promedio_mensual: { $round: ["$promedio_mensual", 1] }
    }
  },

  {
    $set: {
      categoria: {
        $switch: {
          branches: [
            { case: { $lt: ["$promedio_mensual", 2] }, then: "infrequent" },
            {
              case: {
                $and: [
                  { $gte: ["$promedio_mensual", 2] },
                  { $lte: ["$promedio_mensual", 5] }
                ]
              },
              then: "regular"
            },
            { case: { $gt: ["$promedio_mensual", 5] }, then: "frequent" }
          ],
          default: "infrequent"
        }
      }
    }
  },

  {
    $project: {
      _id: 0,
      nombre: 1,
      promedio_mensual: 1,
      categoria: 1
    }
  }
])
