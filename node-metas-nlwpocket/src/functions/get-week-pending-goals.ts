import dayjs from 'dayjs'
import { db } from '../db'
import { goalCompetions, goals } from '../db/schema'
import { and, lte, count, gte, eq, sql } from 'drizzle-orm'

export async function getWeekPendingGoals() {
  const firstDayOfweek = dayjs().startOf('week').toDate()
  const lastDayOfWeek = dayjs().endOf('week').toDate()

  const goalsCreatedUpToWeek = db.$with('goals_created_up_to_week').as(
    db
      .select({
        id: goals.id,
        title: goals.title,
        desiredWeeklyFrequency: goals.desiredWeeklyFrequency,
        createdAt: goals.createdAt,
      })
      .from(goals)
      .where(lte(goals.createdAt, lastDayOfWeek))
  )

  const goalsCompletionCounts = db.$with('goal_completion_counts').as(
    db
      .select({
        goalId: goalCompetions.goalId,
        completionCount: count(goalCompetions.id).as('completionCount'),
      })
      .from(goalCompetions)
      .where(
        and(
          gte(goalCompetions.createdAt, firstDayOfweek),
          lte(goalCompetions.createdAt, lastDayOfWeek)
        )
      )
      .groupBy(goalCompetions.goalId)
  )

  const pendinGoals = await db
    .with(goalsCreatedUpToWeek, goalsCompletionCounts)
    .select({
      id: goalsCreatedUpToWeek.id,
      title: goalsCreatedUpToWeek.title,
      desiredWeeklyFrequency: goalsCreatedUpToWeek.desiredWeeklyFrequency,
      completionCount: sql /*sql*/`
        COALESCE(${goalsCompletionCounts.completionCount}, 0)
      `.mapWith(Number),
    })
    .from(goalsCreatedUpToWeek)
    .leftJoin(
      goalsCompletionCounts,
      eq(goalsCompletionCounts.goalId, goalsCreatedUpToWeek.id)
    )

  return { pendinGoals }
}
