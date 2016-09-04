package jp.co.opst

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

// 極性分析 001
object Practice001 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Practice 001")
    val sc = new SparkContext(conf)
    val workDir = args(0)
    // 極性辞書からMap(単語, 評価値(1 or -1))を生成
    val dic01 = sc.textFile(s"${workDir}/kyokusei_dic_001.txt")
                  .map { _.split("""\s""") }
                  .collect { case a if (a.length >= 2) => (a(0),a(1)) }
                  .map { case (np, s) => if(np.startsWith("ポジ")) (s, 1) else (s, -1) }
                  .collectAsMap
    // データから(対象人物名, 評価値合計)を生成
    val datas = sc.textFile(s"${workDir}/data.txt")
                  .map { _.split("""\s""") }
                  .collect { case a if (a.length >= 2) => (a.head, a.tail.toSeq) }
                  .map { case (name, words) => 
                             val point = words.map { dic01.get(_).getOrElse(0) }
                                              .sum
                             (name, point)
                 }.groupByKey(2)
                  .map { case (name, points) => (name, points.sum) }
    // とりあえず出力
    datas.foreach(println)
  }
}

