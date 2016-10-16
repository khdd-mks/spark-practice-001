package jp.co.opst

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import cats.data.Xor.Right

import com.enjapan.knp._
import com.enjapan.knp.models.Bunsetsu

import java.text.Normalizer

// AKB総選挙 極性分析 001
object AkbAnalyzer001 {
  def main(args: Array[String]) {
    def normalize(s:String) = Normalizer.normalize(s, Normalizer.Form.NFKC)
    def bunsetsuName(os:Option[String]) = os.map{_.split('+').map{_.split('/').head}.mkString}
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
                      .map { s =>
                        KNP.withServerBackedClient { knp =>
                           val parsed = knp.parse(s)
                           parsed.map { blist =>
                             def makeParent(b:Bunsetsu):List[String] = {
                               @annotation.tailrec
                               def inner(innerB:Bunsetsu, l:List[String]):List[String] = 
                                 innerB.parent match {
                                   case Some(parent) => inner(parent, bunsetsuName(parent.repName).getOrElse("文節") :: l)
                                   case None => l
                                 }

                               inner(b, List.empty)
                             }
                             // 文節リストから人名と判断されたものを抽出する
                             val personBunsetsus = blist.bunsetsuList.filter{ _.features.contains("人名") }
                             // 人名リストから人名を抽出
                             val personNames = personBunsetsus.map{ b => bunsetsuName(b.repName) }
                                                              .headOption.flatten.getOrElse("AKBメンバー")
                             // 人名リストから親を抽出
                             val parents = personBunsetsus.map{_.parent.map{makeParent(_)}}
                                                          .collect { case Some(l) => l }
                                                          .flatten
                             // 人名と親をマージ
                             (personNames, parents)
                           }
                       }
                    }.collect { case Right(a) => a }
    val datas = baseDatas
                  .collect { case (name, l) if (l.length >= 1) => (name, l.map(normalize)) }
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

