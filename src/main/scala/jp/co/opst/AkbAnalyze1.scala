package jp.co.opst

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import com.atilika.kuromoji.ipadic._

import java.text.Normalizer

// AKB総選挙 極性分析 001
object AkbAnalyzer001 {
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
    val baseDatas = sc.textFile(s"${workDir}/data.txt")
                      .filter(!_.isEmpty)
                      .map { s =>
                                  import scala.collection.JavaConverters._
                                  val tokenizer = new Tokenizer
                                  val tokenList = tokenizer.tokenize(s).iterator.asScala.toList
                                  // 人物名を特定
                                  val (nameTokens, otherTokens) = tokenList.partition { token =>
                                                      token.getAllFeaturesArray.contains("人名") &&
                                                        token.getSurface != "ちゃん" &&
                                                        token.getSurface != "さん"
                                                                  }
                                  // (人物名, 他の単語のリスト)を生成
                                  (normalize(nameTokens.map{_.getSurface}.mkString), otherTokens.map{_.getSurface}.map(normalize))
                       }.cache
    val datas = baseDatas
                  .filter { case (name, l) => l.length >= 1 }
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
    baseDatas.foreach(println)
    datas.foreach(println)
  }
}

