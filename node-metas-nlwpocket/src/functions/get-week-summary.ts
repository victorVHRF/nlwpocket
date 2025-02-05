import { and, count, desc, eq, gte, lte, sql } from 'drizzle-orm'
import { db } from '../db'
import { goalCompetions, goals } from '../db/schema'
import dayjs from 'dayjs'
import { number } from 'zod'

export async function getWeekSummary() {
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

  const goalsCompletedInWeek = db.$with('goal_completed_in_week').as(
    db
      .select({
        id: goalCompetions.id,
        title: goals.title,
        completedAt: goalCompetions.createdAt,
        completedAtDate: sql /*sql*/`
          DATE(${goalCompetions.createdAt})
        `.as('completedAtDate'),
      })
      .from(goalCompetions)
      .innerJoin(goals, eq(goals.id, goalCompetions.goalId))
      .where(
        and(
          gte(goalCompetions.createdAt, firstDayOfweek),
          lte(goalCompetions.createdAt, lastDayOfWeek)
        )
      )
      .orderBy(desc(goalCompetions.createdAt))
  )

  const goalsCompletedByWeekDay = db.$with('goals_completed_by_week_day').as(
    db
      .select({
        completedAtDate: goalsCompletedInWeek.completedAtDate,
        completions: sql /*sql*/`
          JSON_AGG(
            JSON_BUILD_OBJECT(
              'id', ${goalsCompletedInWeek.id}, 
              'title', ${goalsCompletedInWeek.title}, 
              'completedAt', ${goalsCompletedInWeek.completedAt} 
            )
          )
      `.as('completions'),
      })
      .from(goalsCompletedInWeek)
      .groupBy(goalsCompletedInWeek.completedAtDate)
      .orderBy(desc(goalsCompletedInWeek.completedAtDate))
  )

  type GoalsPerDay = Record<
    string,
    {
      id: string
      title: string
      completedAt: string
    }[]
  >

  const result = await db
    .with(goalsCreatedUpToWeek, goalsCompletedInWeek, goalsCompletedByWeekDay)
    .select({
      completed: sql /*sql */`
        (SELECT COUNT(*) FROM ${goalsCompletedInWeek})
      `.mapWith(Number),
      total: sql /*sql */`
        (SELECT SUM(${goalsCreatedUpToWeek.desiredWeeklyFrequency}) FROM ${goalsCreatedUpToWeek})
      `.mapWith(Number),
      goalsPerDay: sql /*sql*/<GoalsPerDay>`
        JSON_OBJECT_AGG(
          ${goalsCompletedByWeekDay.completedAtDate},
          ${goalsCompletedByWeekDay.completions}
        )
      `,
    })
    .from(goalsCompletedByWeekDay)

  return {
    summary: result[0],
  }
}
