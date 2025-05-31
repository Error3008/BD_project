//1. Średni czas podróży (w godzinach) wszystkich autobusów dla każdej firmy
db.przyjazdy_i_odjazdy.aggregate([
  {
    $addFields: {
      odjazdDate: {
        $dateFromString: {
          dateString: {
            $concat: ["$odjazd.data", "T", "$odjazd.godzina", ":00"]
          }
        }
      },
      przyjazdDate: {
        $dateFromString: {
          dateString: {
            $concat: ["$przyjazd.data", "T", "$przyjazd.godzina", ":00"]
          }
        }
      }
    }
  },
  {
    $addFields: {
      czasPodrozy: {
        $divide: [
          { $subtract: ["$przyjazdDate", "$odjazdDate"] },
          3600000
        ]
      }
    }
  },
  {
    $group: {
      _id: "$firma",
      sredni_czas_podrozy: { $avg: "$czasPodrozy" }
    }
  },
  {
    $project: {
      _id: 0,
      firma: "$_id",
      sredni_czas_podrozy: { $round: ["$sredni_czas_podrozy", 1] }
    }
  },
  {
    $sort: { sredni_czas_podrozy: -1 }
  }
])
//2. Statystyki dotyczące liczby pasażerów i rejsów dla każdego stanowiska odjazdu
db.przyjazdy_i_odjazdy.aggregate([
  {
    $lookup: {
      from: "bilety",
      let: {
        rejs: "$numer_rejsu",
        miasto_odjazdu: "$odjazd.miasto"
      },
      pipeline: [
        {
          $match: {
            $expr: {
              $and: [
                { $eq: ["$numer_rejsu", "$$rejs"] },
                { $eq: ["$odjazd.miasto", "$$miasto_odjazdu"] }
              ]
            }
          }
        }
      ],
      as: "dopasowane_bilety"
    }
  },
  {
    $unwind: {
      path: "$dopasowane_bilety",
      preserveNullAndEmptyArrays: true
    }
  },
  {
    $group: {
      _id: {
        miasto: "$odjazd.miasto",
        rejs_id: "$id"
      },
      liczba_pasazerow: { $sum: 1 }
    }
  },
  {
    $group: {
      _id: "$_id.miasto",
      liczba_rejsow: { $sum: 1 },
      liczba_pasazerow: { $sum: "$liczba_pasazerow" }
    }
  },
  {
    $project: {
      _id: 0,
      odjazd__miasto: "$_id",
      liczba_rejsow: 1,
      liczba_pasazerow: 1,
      srednio_pasazerow_na_rejs: {
        $round: [
          { $divide: ["$liczba_pasazerow", "$liczba_rejsow"] },
          2
        ]
      }
    }
  },
  {
    $match: {
      srednio_pasazerow_na_rejs: { $gt: 1.0 }
    }
  },
  {
    $sort: { liczba_pasazerow: -1 }
  }
])
//3. Dla każdej trasy (miasto wyjazdu → miasto przyjazdu): liczba pasażerów i najpopularniejszy rejs
db.bilety.aggregate([
  {
    $group: {
      _id: {
        odjazd_miasto: "$odjazd.miasto",
        przyjazd_miasto: "$przyjazd.miasto",
        numer_rejsu: "$numer_rejsu"
      },
      liczba_pasazerow: { $sum: 1 }
    }
  },
  {
    $project: {
      _id: 0,
      "odjazd__miasto": "$_id.odjazd_miasto",
      "przyjazd__miasto": "$_id.przyjazd_miasto",
      "numer_rejsu": "$_id.numer_rejsu",
      "liczba_pasazerow": 1
    }
  },
  {
    $sort: { liczba_pasazerow: -1 }
  }
])
//4. Statystyki firmy: liczba rejsów, liczba pasażerów, średnia liczba pasażerów na rejs
db.przyjazdy_i_odjazdy.aggregate([
  {
    $lookup: {
      from: "bilety",
      localField: "numer_rejsu",
      foreignField: "numer_rejsu",
      as: "dopasowane_bilety"
    }
  },
  {
    $unwind: {
      path: "$dopasowane_bilety",
      preserveNullAndEmptyArrays: true
    }
  },
  {
    $group: {
      _id: "$firma",
      unikalne_rejsy: { $addToSet: "$numer_rejsu" },
      liczba_pasazerow: { $sum: { $cond: [{ $ne: ["$dopasowane_bilety", null] }, 1, 0] } }
    }
  },
  {
    $project: {
      firma: "$_id",
      liczba_rejsow: { $size: "$unikalne_rejsy" },
      liczba_pasazerow: 1,
      srednio_pasazerow_na_rejs: {
        $round: [
          { $divide: ["$liczba_pasazerow", { $size: "$unikalne_rejsy" }] },
          2
        ]
      }
    }
  },
  {
    $match: {
      liczba_pasazerow: { $gt: 0 }
    }
  },
  {
    $sort: { srednio_pasazerow_na_rejs: -1 }
  }
])