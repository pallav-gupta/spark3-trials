package org.apache.spark.joins

import java.util.Locale

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{
  Attribute,
  Expression,
  GenericInternalRow,
  JoinedRow,
  Predicate,
  PredicateHelper,
  UnsafeProjection
}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.plans.physical.{
  BroadcastDistribution,
  Distribution,
  IdentityBroadcastMode,
  UnspecifiedDistribution
}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{BinaryExecNode, SparkPlan}
import org.apache.spark.sql.execution.joins.{BuildLeft, BuildRight, BuildSide}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.util.collection.{BitSet, CompactBuffer}

case class BroadcastNestedLoopBreakOnFirstMatchJoin(
  left: SparkPlan,
  right: SparkPlan,
  buildSide: BuildSide,
  joinType: JoinType,
  condition: Option[Expression]
) extends BinaryExecNode {

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics
      .createMetric(sparkContext, "number of output rows")
  )

  /** BuildRight means the right relation <=> the broadcast relation. */
  private val (streamed, broadcast) = buildSide match {
    case BuildRight => (left, right)
    case BuildLeft  => (right, left)
  }

  override def requiredChildDistribution: Seq[Distribution] = buildSide match {
    case BuildLeft =>
      BroadcastDistribution(IdentityBroadcastMode) :: UnspecifiedDistribution :: Nil
    case BuildRight =>
      UnspecifiedDistribution :: BroadcastDistribution(IdentityBroadcastMode) :: Nil
  }

  private[this] def genResultProjection: UnsafeProjection = joinType match {
    case LeftExistence(j) =>
      UnsafeProjection.create(output, output)
    case other =>
      // Always put the stream side on left to simplify implementation
      // both of left and right side could be null
      UnsafeProjection.create(
        output,
        (streamed.output ++ broadcast.output).map(_.withNullability(true))
      )
  }

  override def output: Seq[Attribute] = {
    joinType match {
      case _: InnerLike =>
        left.output ++ right.output
      case LeftOuter =>
        left.output ++ right.output.map(_.withNullability(true))
      case RightOuter =>
        left.output.map(_.withNullability(true)) ++ right.output
      case FullOuter =>
        left.output.map(_.withNullability(true)) ++ right.output.map(
          _.withNullability(true)
        )
      case j: ExistenceJoin =>
        left.output :+ j.exists
      case LeftExistence(_) =>
        left.output
      case x =>
        throw new IllegalArgumentException(
          s"BroadcastNestedLoopJoin should not take $x as the JoinType"
        )
    }
  }

  @transient private lazy val boundCondition = {
    if (condition.isDefined) {
      Predicate
        .create(condition.get, streamed.output ++ broadcast.output)
        .eval _
    } else { (r: InternalRow) =>
      true
    }
  }

  /**
    * The implementation for InnerJoin.
    */
  private def innerJoin(
    relation: Broadcast[Array[InternalRow]]
  ): RDD[InternalRow] = {
    streamed.execute().mapPartitionsInternal { streamedIter =>
      val buildRows = relation.value
      val joinedRow = new JoinedRow

      streamedIter.flatMap { streamedRow =>
        val joinedRows = buildRows.iterator.map(r => joinedRow(streamedRow, r))
        if (condition.isDefined) {
          val result = joinedRows.find(boundCondition).getOrElse(null)
          if(result!=null) Iterator(result) else Iterator.empty
        } else {
          joinedRows
        }
      }
    }
  }

  /**
    * The implementation for these org.apache.spark.joins:
    *
    *   LeftOuter with BuildRight
    *   RightOuter with BuildLeft
    */
  private def outerJoin(
    relation: Broadcast[Array[InternalRow]]
  ): RDD[InternalRow] = {
    streamed.execute().mapPartitionsInternal { streamedIter =>
      val buildRows = relation.value
      val joinedRow = new JoinedRow
      val nulls = new GenericInternalRow(broadcast.output.size)

      // Returns an iterator to avoid copy the rows.
      new Iterator[InternalRow] {
        // current row from stream side
        private var streamRow: InternalRow = null
        // have found a match for current row or not
        private var foundMatch: Boolean = false
        // the matched result row
        private var resultRow: InternalRow = null
        // the next index of buildRows to try
        private var nextIndex: Int = 0

        private def findNextMatch(): Boolean = {
          if (streamRow == null) {
            if (!streamedIter.hasNext) {
              return false
            }
            streamRow = streamedIter.next()
            nextIndex = 0
            foundMatch = false
          }
          while (nextIndex < buildRows.length) {
            resultRow = joinedRow(streamRow, buildRows(nextIndex))
            nextIndex += 1
            if (boundCondition(resultRow)) {
              foundMatch = true
              streamRow = null
              return true
            }
          }
          if (!foundMatch) {
            resultRow = joinedRow(streamRow, nulls)
            streamRow = null
            true
          } else {
            resultRow = null
            streamRow = null
            findNextMatch()
          }
        }

        override def hasNext(): Boolean = {
          resultRow != null || findNextMatch()
        }
        override def next(): InternalRow = {
          val r = resultRow
          resultRow = null
          r
        }
      }
    }
  }

  /**
    * The implementation for these org.apache.spark.joins:
    *
    *   LeftSemi with BuildRight
    *   Anti with BuildRight
    */
  private def leftExistenceJoin(relation: Broadcast[Array[InternalRow]],
                                exists: Boolean): RDD[InternalRow] = {
    assert(buildSide == BuildRight)
    streamed.execute().mapPartitionsInternal { streamedIter =>
      val buildRows = relation.value
      val joinedRow = new JoinedRow

      if (condition.isDefined) {
        val result = streamedIter
          .find(
            l =>
              buildRows.exists(r => boundCondition(joinedRow(l, r))) == exists
          )
          .getOrElse(null)
        if (result != null) Iterator(result) else Iterator.empty

      } else if (buildRows.nonEmpty == exists) {
        streamedIter
      } else {
        Iterator.empty
      }
    }
  }

  private def existenceJoin(
    relation: Broadcast[Array[InternalRow]]
  ): RDD[InternalRow] = {
    assert(buildSide == BuildRight)
    streamed.execute().mapPartitionsInternal { streamedIter =>
      val buildRows = relation.value
      val joinedRow = new JoinedRow

      if (condition.isDefined) {
        val resultRow = new GenericInternalRow(Array[Any](null))
        streamedIter.map { row =>
          val result = buildRows.exists(r => boundCondition(joinedRow(row, r)))
          resultRow.setBoolean(0, result)
          joinedRow(row, resultRow)
        }
      } else {
        val resultRow = new GenericInternalRow(Array[Any](buildRows.nonEmpty))
        streamedIter.map { row =>
          joinedRow(row, resultRow)
        }
      }
    }
  }

  /**
    * The implementation for these org.apache.spark.joins:
    *
    *   LeftOuter with BuildLeft
    *   RightOuter with BuildRight
    *   FullOuter
    *   LeftSemi with BuildLeft
    *   LeftAnti with BuildLeft
    *   ExistenceJoin with BuildLeft
    */
  private def defaultJoin(
    relation: Broadcast[Array[InternalRow]]
  ): RDD[InternalRow] = {

    /** All rows that either match both-way, or rows from streamed joined with nulls. */
    val streamRdd = streamed.execute()

    val matchedBuildRows = streamRdd.mapPartitionsInternal { streamedIter =>
      val buildRows = relation.value
      val matched = new BitSet(buildRows.length)
      val joinedRow = new JoinedRow

      streamedIter.foreach { streamedRow =>
        var i = 0
        var matchFound = false
          while (i < buildRows.length && !matchFound) {
            if(boundCondition(joinedRow(streamedRow, buildRows(i)))) {
              matched.set(i)
              matchFound = true
            }
            i += 1
          }
      }
      Seq(matched).toIterator
    }

    val matchedBroadcastRows =
      matchedBuildRows.fold(new BitSet(relation.value.length))(_ | _)

    joinType match {
      case LeftSemi =>
        assert(buildSide == BuildLeft)
        val buf: CompactBuffer[InternalRow] = new CompactBuffer()
        var i = 0
        val rel = relation.value
        while (i < rel.length) {
          if (matchedBroadcastRows.get(i)) {
            buf += rel(i).copy()
          }
          i += 1
        }
        return sparkContext.makeRDD(buf)
      case j: ExistenceJoin =>
        val buf: CompactBuffer[InternalRow] = new CompactBuffer()
        var i = 0
        val rel = relation.value
        while (i < rel.length) {
          val result = new GenericInternalRow(
            Array[Any](matchedBroadcastRows.get(i))
          )
          buf += new JoinedRow(rel(i).copy(), result)
          i += 1
        }
        return sparkContext.makeRDD(buf)
      case LeftAnti =>
        val notMatched: CompactBuffer[InternalRow] = new CompactBuffer()
        var i = 0
        val rel = relation.value
        while (i < rel.length) {
          if (!matchedBroadcastRows.get(i)) {
            notMatched += rel(i).copy()
          }
          i += 1
        }
        return sparkContext.makeRDD(notMatched)
      case o =>
    }

    val notMatchedBroadcastRows: Seq[InternalRow] = {
      val nulls = new GenericInternalRow(streamed.output.size)
      val buf: CompactBuffer[InternalRow] = new CompactBuffer()
      val joinedRow = new JoinedRow
      joinedRow.withLeft(nulls)
      var i = 0
      val buildRows = relation.value
      while (i < buildRows.length) {
        if (!matchedBroadcastRows.get(i)) {
          buf += joinedRow.withRight(buildRows(i)).copy()
        }
        i += 1
      }
      buf
    }

    val matchedStreamRows = streamRdd.mapPartitionsInternal { streamedIter =>
      val buildRows = relation.value
      val joinedRow = new JoinedRow
      val nulls = new GenericInternalRow(broadcast.output.size)

      streamedIter.flatMap { streamedRow =>
        var i = 0
        var foundMatch = false
        val matchedRows = new CompactBuffer[InternalRow]

        while (i < buildRows.length && !foundMatch) {
          if(boundCondition(joinedRow(streamedRow, buildRows(i)))) {
            matchedRows += joinedRow.copy()
            foundMatch = true
          }
            i += 1
        }

        if (!foundMatch && joinType == FullOuter) {
          matchedRows += joinedRow(streamedRow, nulls).copy()
        }
        matchedRows.iterator
      }
    }

    sparkContext.union(
      matchedStreamRows,
      sparkContext.makeRDD(notMatchedBroadcastRows)
    )
  }

  protected override def doExecute(): RDD[InternalRow] = {
    val broadcastedRelation = broadcast.executeBroadcast[Array[InternalRow]]()

    val resultRdd = (joinType, buildSide) match {
      case (_: InnerLike, _) =>
        innerJoin(broadcastedRelation)
      case (LeftOuter, BuildRight) | (RightOuter, BuildLeft) =>
        outerJoin(broadcastedRelation)
      case (LeftSemi, BuildRight) =>
        leftExistenceJoin(broadcastedRelation, exists = true)
      case (LeftAnti, BuildRight) =>
        leftExistenceJoin(broadcastedRelation, exists = false)
      case (j: ExistenceJoin, BuildRight) =>
        existenceJoin(broadcastedRelation)
      case _ =>
        /**
          * LeftOuter with BuildLeft
          * RightOuter with BuildRight
          * FullOuter
          * LeftSemi with BuildLeft
          * LeftAnti with BuildLeft
          * ExistenceJoin with BuildLeft
          */
        defaultJoin(broadcastedRelation)
    }

    val numOutputRows = longMetric("numOutputRows")
    resultRdd.mapPartitionsWithIndexInternal { (index, iter) =>
      val resultProj = genResultProjection
      resultProj.initialize(index)
      iter.map { r =>
        numOutputRows += 1
        resultProj(r)
      }
    }
  }
}

object BroadCastLoopBreakJoinSelection extends Strategy with PredicateHelper {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case j @ logical
          .Join(left, right, joinType, condition, hint)
        if (left find(node => node.isInstanceOf[CustomResolvedHint])).isDefined => {
      val buildSide = BuildRight
      right.conf
      val newLeft = left transform {
        case hint: CustomResolvedHint =>
         hint.child
      }
      BroadcastNestedLoopBreakOnFirstMatchJoin(
        planLater(newLeft),
        planLater(right),
        buildSide,
        joinType,
        condition
      ) :: Nil

    }

    // --- Cases where this strategy does not apply ---------------------------------------------
    case _ => Nil
  }

  def canBroadcastBySizes(joinType: JoinType,
                          left: LogicalPlan,
                          right: LogicalPlan): Boolean = {
    val buildLeft = canBuildLeft(joinType) && canBroadcast(left)
    val buildRight = canBuildRight(joinType) && canBroadcast(right)
    buildLeft || buildRight
  }

  def canBroadcast(plan: LogicalPlan): Boolean = {
    plan.stats.sizeInBytes >= 0 && plan.stats.sizeInBytes <= plan.conf.autoBroadcastJoinThreshold

  }

  private def canBuildRight(joinType: JoinType): Boolean = joinType match {
    case _: InnerLike | LeftOuter | LeftSemi | LeftAnti | _: ExistenceJoin =>
      true
    case _ => false
  }

  private def canBuildLeft(joinType: JoinType): Boolean = joinType match {
    case _: InnerLike | RightOuter => true
    case _                         => false
  }

  def broadcastSideBySizes(joinType: JoinType,
                           left: LogicalPlan,
                           right: LogicalPlan): BuildSide = {
    val buildLeft = canBuildLeft(joinType) && canBroadcast(left)
    val buildRight = canBuildRight(joinType) && canBroadcast(right)
    broadcastSide(buildLeft, buildRight, left, right)
  }

  def broadcastSide(canBuildLeft: Boolean,
                    canBuildRight: Boolean,
                    left: LogicalPlan,
                    right: LogicalPlan): BuildSide = {

    def smallerSide =
      if (right.stats.sizeInBytes <= left.stats.sizeInBytes) BuildRight
      else BuildLeft

    if (canBuildRight && canBuildLeft) {
      // Broadcast smaller side base on its estimated physical size
      // if both sides have broadcast hint
      smallerSide
    } else if (canBuildRight) {
      BuildRight
    } else if (canBuildLeft) {
      BuildLeft
    } else {
      // for the last default broadcast nested loop join
      smallerSide
    }
  }
}

object BroadCastHint extends Rule[LogicalPlan] {

  override def apply(logicalPlan: LogicalPlan): LogicalPlan =
    logicalPlan transformUp {
      case hint: UnresolvedHint
          if "SKR".equalsIgnoreCase(hint.name.toUpperCase(Locale.ROOT)) => {

        CustomResolvedHint(hint.child, HintInfo("BREAK"))

      }
    }

  /*override def preOptimizationBatches : Seq[Batch] = {

  }
 */

}

case class HintInfo(name: String = null) {

  def merge(other: HintInfo, hintErrorHandler: HintErrorHandler): HintInfo = {

    HintInfo()
  }

  override def toString: String = name
}

case class CustomResolvedHint(child: LogicalPlan, hintInfo: HintInfo)
    extends UnaryNode {

  override def output: Seq[Attribute] = child.output

  override def doCanonicalize(): LogicalPlan = child.canonicalized
}

case class Join(left: LogicalPlan,
                right: LogicalPlan,
                joinType: JoinType,
                condition: Option[Expression],
                hint: JoinHint)
