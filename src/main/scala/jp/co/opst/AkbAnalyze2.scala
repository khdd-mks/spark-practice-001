package jp.co.opst

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf

import com.atilika.kuromoji.ipadic._

import java.text.Normalizer

trait Word
trait Target

case class WordEvaluate(word:MeantString[Word], target:MeantString[Target], evaluate: Evaluate, existDic: Boolean)

// AKB総選挙 極性分析 002
object AkbAnalyzer002 extends App {
    def normalize(s:String) = Normalizer.normalize(s, Normalizer.Form.NFKC)
    val conf = new SparkConf().setAppName("Practice 002")
    val session = SparkSession.builder().config(conf).getOrCreate()
    val sc = session.sparkContext
    val sqlc = session.sqlContext
    val workDir = args.head
    // 極性辞書からMap(単語, 評価値(1 or -1))を生成
    val dic01 = sc.textFile(s"${workDir}/kyokusei_dic_001.txt")
                    .map { _.split("""\s""") }
                    .flatMap { a => if (a.length >= 2) Some((a.tail.mkString, a.head)) else None }
                    .map { case (s, np) => if(np.startsWith("ポジ")) (s, 1L) else (s, -1L) }
                    .map { case (s, np) => (normalize(s), np) }
    val dic02 = sc.textFile(s"${workDir}/kyokusei_dic_002.txt")
                  .map { _.split("""\s""") }
                  .collect { case a if (a.length >= 2) => (a.head,a.tail.head) }
                  .collect { case (s, nep) if nep != "e" => if(nep == "p") (s, 1L) else (s, -1L) }
                  .map { case (s, np) => (normalize(s), np) }
    // 解析対象データを一旦(単語, 人物名)のリストに変換
    val baseDatas = sc.textFile(s"${workDir}/data.txt")
                      .filter(!_.isEmpty)
                      .flatMap { s =>
                                  import scala.collection.JavaConverters._
                                  val tokenizer = {
                                    val builder = new Tokenizer.Builder()
                                    builder.userDictionary(s"${workDir}/akbdic.txt")
                                    builder.build
                                  }
                                  val tokenList = tokenizer.tokenize(s).iterator.asScala.toList
                                  // 人物名を特定
                                  val (nameTokens, otherTokens) = tokenList.partition { token =>
                                                      (token.getAllFeaturesArray.contains("人名") ||
                                                        token.getAllFeaturesArray.contains("カスタム人名")) &&
                                                        token.getSurface != "ちゃん" &&
                                                        token.getSurface != "さん"
                                                                  }
                                  // (単語, 人物名)リストを生成
                                  val fixedNameTokens = normalize(nameTokens.map{_.getSurface}.mkString)
                                  otherTokens.map{_.getSurface}.map(normalize).map((_, fixedNameTokens))
                       }.cache
    // データから(対象人物名, 評価値合計, 辞書に存在した単語数)を生成
    val datas = {
      val evaluateRdd = baseDatas
        .leftOuterJoin(dic01)
        .leftOuterJoin(dic02)
        .map { case (word, ((name, oEvaluate1), oEvaluate2)) =>
          WordEvaluate(
            word, name,
            oEvaluate1.getOrElse(0L) + oEvaluate2.getOrElse(0L),
            oEvaluate1.orElse(oEvaluate2).isDefined)
        }
      import sqlc.implicits._
      val evaluateDataset = sqlc.createDataset(evaluateRdd)
      evaluateDataset
        .groupByKey(_.target)
        .mapGroups { case (target, evaluates) =>
          val (evalIter, wordIter) = evaluates.duplicate
          val evalSum = evalIter.map(_.evaluate).sum
          val wordCount = wordIter.count(_.existDic)
          (target, evalSum, wordCount)
        }
    }

    // とりあえず出力
    baseDatas.foreach(println)
    datas.rdd.foreach(println)
}

