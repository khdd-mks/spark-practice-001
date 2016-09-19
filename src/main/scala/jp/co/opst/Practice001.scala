package jp.co.opst

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import java.text.Normalizer

// 極性分析 001
object Practice001 {
  def main(args: Array[String]) {
    def normalize(s:String) = Normalizer.normalize(s, Normalizer.Form.NFKC)
    val conf = new SparkConf().setAppName("Practice 001")
    val sc = new SparkContext(conf)
    val workDir = args.head
    // 極性辞書からMap(単語, 評価値(1 or -1))を生成
    val dic01 = sc.textFile(s"${workDir}/kyokusei_dic_001.txt")
                  .map { _.split("""\s""") }
                  .collect { case a if (a.length >= 2) => (a.head,a.tail.mkString) }
                  .map { case (np, s) => if(np.startsWith("ポジ")) (s, 1) else (s, -1) }
                  .map { case (s, np) => (normalize(s), np) }
                  .collectAsMap
    val dic02 = sc.textFile(s"${workDir}/kyokusei_dic_002.txt")
                  .map { _.split("""\s""") }
                  .collect { case a if (a.length >= 2) => (a.head,a.tail.head) }
                  .collect { case (s, nep) if nep != "e" => if(nep == "p") (s, 1) else (s, -1) }
                  .map { case (s, np) => (normalize(s), np) }
                  .collectAsMap
    // データから(対象人物名, 評価値合計, 辞書に存在した単語数)を生成
    val datas = sc.textFile(s"${workDir}/data.txt")
                  .map { _.split("""\s""") }
                  .collect { case a if (a.length >= 2) => (a.head, a.tail.map(normalize).toSeq) }
                  .map { case (name, words) => 
                             val (points, hits) = words.map { s =>
                                                                val oHit = dic01.get(s).orElse(dic02.get(s))
                                                                (oHit.getOrElse(0), oHit.map { _ => 1 }.getOrElse(0))
                                                      }.unzip
                             (name, (points.sum, hits.sum))
                 }.groupByKey(2)
                  .map { case (name, iter) =>
                             val (points, hits) = iter.unzip
                             (name, points.sum, hits.sum)
                 }
    // とりあえず出力
    datas.foreach(println)
  }
}

