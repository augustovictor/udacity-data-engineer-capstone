import findspark
findspark.init()
import configparser
import os
from datetime import datetime
from typing import List

from pyspark.sql import SparkSession
from pyspark.sql import functions as  F
from pyspark.sql.functions import udf
from pyspark.sql.types import TimestampType, BooleanType

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']
AWS_BUCKET = config['AWS']['AWS_BUCKET']
AWS_RAW_DATA_KEY = config['AWS']['AWS_RAW_DATA_KEY']
S3_TRANSFORMED_RAW_DATA_KEY = config['AWS']['S3_TRANSFORMED_RAW_DATA_KEY']
AWS_MATCH_KEY=config['AWS']['AWS_MATCH_KEY']
MAX_PARQUET_PART_SIZE_IN_MB = 4
MAX_PARQUET_PART_SIZE_IN_BYTES = MAX_PARQUET_PART_SIZE_IN_MB * 1000000


def get_s3_path_for(aws_bucket: str, aws_data_keys: List[str]) -> str:
    keys = "/".join(aws_data_keys)
    return f"s3a://{aws_bucket}/{keys}"


@udf(TimestampType())
def get_datetime_from(long_value):
    """
    Converts timestamp of type Long to datetime
    """

    return datetime.fromtimestamp(long_value / 1000.0)


@udf(BooleanType())
def get_converted_str_to_boolean(boolean_string: str) -> bool:
    """
    Converts string to boolean
    """

    return boolean_string == "Win"


def create_spark_session() -> SparkSession:
    """
    Creates and configures spark session
    """
    spark = (
        SparkSession.builder
        .master("local[*]")
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0")
        .config("spark.driver.memory", "10g")
        # .config("spark.sql.files.maxPartitionBytes", MAX_PARQUET_PART_SIZE_IN_BYTES)
        .enableHiveSupport()
        .getOrCreate())

    spark.conf.set("mapreduce.fileoutputcommitter.algorithm.version", "2")

    return spark


def process_summoners_data(
        spark: SparkSession,
        s3_raw_data_path: str,
        s3_destination_path: str
):
    """
    Processes summoners data
    Loads data into DataLake in parquet format
    """
    pass


def process_champions_data(
        spark: SparkSession,
        s3_raw_data_path: str,
        s3_destination_path: str
):
    """
    Processes champions data
    Loads data into DataLake in parquet format
    """
    pass


def process_items_data(
        spark: SparkSession,
        s3_raw_data_path: str,
        s3_destination_path: str
):
    """
    Processes items data
    Loads data into DataLake in parquet format
    """
    pass


def process_matches_data(
        spark: SparkSession,
        s3_raw_data_path: str,
        s3_destination_path: str
):
    """
    Processes matches data
    Loads data into DataLake in parquet format
    """
    s3_raw_data_path = "../../riot-scraper/results/matches"
    # s3_destination_path = "../data/parquet/matches_data"

    raw_data = spark.read.json(s3_raw_data_path)

    raw_data_formatted = (
        raw_data
        .withColumn("team", F.explode(F.col("teams")))
        .withColumn("participant", F.explode(F.col("participants")))
        .withColumn("participantIdentity", F.explode(F.col("participantIdentities")))
        .withColumn('ts', get_datetime_from(raw_data.gameCreation))
        .withColumn('year', F.year('ts'))
        .withColumn('month', F.month('ts'))
        .select(
            F.col("gameCreation").alias("game_creation"),
            F.col("gameDuration").alias("game_duration"),
            F.col("gameId").alias("game_id"),
            F.col("gameMode").alias("game_mode"),
            F.col("gameType").alias("game_type"),
            F.col("team.teamId").alias("team_team_id"),
            F.col("participant.participantId").alias("participant_participant_id"),
            F.col("team.win").alias("team_win"),
            F.col("team.firstBlood").alias("team_first_blood"),
            F.col("team.firstTower").alias("team_first_tower"),
            F.col("team.firstInhibitor").alias("team_first_inhibitor"),
            F.col("team.firstBaron").alias("team_first_baron"),
            F.col("team.firstDragon").alias("team_first_dragon"),
            F.col("team.firstRiftHerald").alias("team_first_rift_herald"),
            F.col("team.towerKills").alias("team_tower_kills"),
            F.col("team.baronKills").alias("team_baron_kills"),
            F.col("team.dragonKills").alias("team_dragon_kills"),
            F.col("team.vilemawKills").alias("team_vilemaw_kills"),
            F.col("team.riftHeraldKills").alias("team_rift_herald_kills"),
            F.col("participant.championId").alias("participant_champion_id"),
            F.col("participant.spell1Id").alias("participant_spell1_id"),
            F.col("participant.spell2Id").alias("participant_spell2_id"),
            F.col("participant.stats.win").alias("participant_stats_win"),
            F.col("participant.stats.item0").alias("participant_stats_item0"),
            F.col("participant.stats.item1").alias("participant_stats_item1"),
            F.col("participant.stats.item2").alias("participant_stats_item2"),
            F.col("participant.stats.item3").alias("participant_stats_item3"),
            F.col("participant.stats.item4").alias("participant_stats_item4"),
            F.col("participant.stats.item5").alias("participant_stats_item5"),
            F.col("participant.stats.item6").alias("participant_stats_item6"),
            F.col("participant.stats.kills").alias("participant_stats_kills"),
            F.col("participant.stats.deaths").alias("participant_stats_deaths"),
            F.col("participant.stats.assists").alias("participant_stats_assists"),
            F.col("participant.stats.largestKillingSpree").alias("participant_stats_largest_killing_spree"),
            F.col("participant.stats.largestMultiKill").alias("participant_stats_largest_multi_kill"),
            F.col("participant.stats.killingSprees").alias("participant_stats_killing_sprees"),
            F.col("participant.stats.longestTimeSpentLiving").alias("participant_stats_longest_time_spent_living"),
            F.col("participant.stats.doubleKills").alias("participant_stats_double_kills"),
            F.col("participant.stats.tripleKills").alias("participant_stats_triple_kills"),
            F.col("participant.stats.quadraKills").alias("participant_stats_quadra_kills"),
            F.col("participant.stats.pentaKills").alias("participant_stats_penta_kills"),
            F.col("participant.stats.unrealKills").alias("participant_stats_unreal_kills"),
            F.col("participant.stats.totalDamageDealt").alias("participant_stats_total_damage_dealt"),
            F.col("participant.stats.magicDamageDealt").alias("participant_stats_magic_damage_dealt"),
            F.col("participant.stats.physicalDamageDealt").alias("participant_stats_physical_damage_dealt"),
            F.col("participant.stats.trueDamageDealt").alias("participant_stats_true_damage_dealt"),
            F.col("participant.stats.largestCriticalStrike").alias("participant_stats_largest_critical_strike"),
            F.col("participant.stats.totalDamageDealtToChampions").alias("participant_stats_total_damage_dealt_to_champions"),
            F.col("participant.stats.magicDamageDealtToChampions").alias("participant_stats_magic_damage_dealt_to_champions"),
            F.col("participant.stats.physicalDamageDealtToChampions").alias("participant_stats_physical_damage_dealt_to_champions"),
            F.col("participant.stats.trueDamageDealtToChampions").alias("participant_stats_true_damage_dealt_to_champions"),
            F.col("participant.stats.totalHeal").alias("participant_stats_total_heal"),
            F.col("participant.stats.totalUnitsHealed").alias("participant_stats_total_units_healed"),
            F.col("participant.stats.damageSelfMitigated").alias("participant_stats_damage_self_mitigated"),
            F.col("participant.stats.damageDealtToObjectives").alias("participant_stats_damage_dealt_to_objectives"),
            F.col("participant.stats.damageDealtToTurrets").alias("participant_stats_damage_dealt_to_turrets"),
            F.col("participant.stats.totalDamageTaken").alias("participant_stats_total_damage_taken"),
            F.col("participant.stats.magicalDamageTaken").alias("participant_stats_magical_damage_taken"),
            F.col("participant.stats.physicalDamageTaken").alias("participant_stats_physical_damage_taken"),
            F.col("participant.stats.trueDamageTaken").alias("participant_stats_true_damage_taken"),
            F.col("participant.stats.goldEarned").alias("participant_stats_gold_earned"),
            F.col("participant.stats.goldSpent").alias("participant_stats_gold_spent"),
            F.col("participant.stats.turretKills").alias("participant_stats_turret_kills"),
            F.col("participant.stats.inhibitorKills").alias("participant_stats_inhibitor_kills"),
            F.col("participant.stats.totalMinionsKilled").alias("participant_stats_total_minions_killed"),
            F.col("participant.stats.neutralMinionsKilled").alias("participant_stats_neutral_minions_killed"),
            F.col("participant.stats.neutralMinionsKilledTeamJungle").alias("participant_stats_neutral_minions_killed_team_jungle"),
            F.col("participant.stats.neutralMinionsKilledEnemyJungle").alias("participant_stats_neutral_minions_killed_enemy_jungle"),
            F.col("participant.stats.totalTimeCrowdControlDealt").alias("participant_stats_total_time_crowd_control_dealt"),
            F.col("participant.stats.champLevel").alias("participant_stats_champ_level"),
            F.col("participant.stats.visionWardsBoughtInGame").alias("participant_stats_vision_wards_bought_in_game"),
            F.col("participant.stats.sightWardsBoughtInGame").alias("participant_stats_sight_wards_bought_in_game"),
            F.col("participant.stats.wardsPlaced").alias("participant_stats_wards_placed"),
            F.col("participant.stats.wardsKilled").alias("participant_stats_wards_killed"),
            F.col("participant.stats.firstBloodKill").alias("participant_stats_first_blood_kill"),
            F.col("participant.stats.firstBloodAssist").alias("participant_stats_first_blood_assist"),
            F.col("participant.stats.firstTowerKill").alias("participant_stats_first_tower_kill"),
            F.col("participant.stats.firstTowerAssist").alias("participant_stats_first_tower_assist"),
            F.col("participant.stats.firstInhibitorKill").alias("participant_stats_first_inhibitor_kill"),
            F.col("participant.stats.firstInhibitorAssist").alias("participant_stats_first_inhibitor_assist"),
            F.col("participant.timeline.creepsPerMinDeltas.0-10").alias("creeps_per_min_deltas_0_10"),
            F.col("participant.timeline.creepsPerMinDeltas.10-20").alias("creeps_per_min_deltas_10_20"),
            F.col("participant.timeline.creepsPerMinDeltas.20-30").alias("creeps_per_min_deltas_20_30"),
            F.col("participant.timeline.creepsPerMinDeltas.30-end").alias("creeps_per_min_deltas_30_end"),
            F.col("participant.timeline.xpPerMinDeltas.0-10").alias("xp_per_min_deltas_0_10"),
            F.col("participant.timeline.xpPerMinDeltas.10-20").alias("xp_per_min_deltas_10_20"),
            F.col("participant.timeline.xpPerMinDeltas.20-30").alias("xp_per_min_deltas_20_30"),
            F.col("participant.timeline.xpPerMinDeltas.30-end").alias("xp_per_min_deltas_30_end"),
            F.col("participant.timeline.goldPerMinDeltas.0-10").alias("gold_per_min_deltas_0_10"),
            F.col("participant.timeline.goldPerMinDeltas.10-20").alias("gold_per_min_deltas_10_20"),
            F.col("participant.timeline.goldPerMinDeltas.20-30").alias("gold_per_min_deltas_20_30"),
            F.col("participant.timeline.goldPerMinDeltas.30-end").alias("gold_per_min_deltas_30_end"),
            F.col("participant.timeline.csDiffPerMinDeltas.0-10").alias("cs_diff_per_min_deltas_0_10"),
            F.col("participant.timeline.csDiffPerMinDeltas.10-20").alias("cs_diff_per_min_deltas_10_20"),
            F.col("participant.timeline.csDiffPerMinDeltas.20-30").alias("cs_diff_per_min_deltas_20_30"),
            F.col("participant.timeline.csDiffPerMinDeltas.30-end").alias("cs_diff_per_min_deltas_30_end"),
            F.col("participant.timeline.xpDiffPerMinDeltas.0-10").alias("xp_diff_per_min_deltas_0_10"),
            F.col("participant.timeline.xpDiffPerMinDeltas.10-20").alias("xp_diff_per_min_deltas_10_20"),
            F.col("participant.timeline.xpDiffPerMinDeltas.20-30").alias("xp_diff_per_min_deltas_20_30"),
            F.col("participant.timeline.xpDiffPerMinDeltas.30-end").alias("xp_diff_per_min_deltas_30_end"),
            F.col("participant.timeline.damageTakenPerMinDeltas.0-10").alias("damage_taken_per_min_deltas_0_10"),
            F.col("participant.timeline.damageTakenPerMinDeltas.10-20").alias("damage_taken_per_min_deltas_10_20"),
            F.col("participant.timeline.damageTakenPerMinDeltas.20-30").alias("damage_taken_per_min_deltas_20_30"),
            F.col("participant.timeline.damageTakenPerMinDeltas.30-end").alias("damage_taken_per_min_deltas_30_end"),
            F.col("participant.timeline.damageTakenDiffPerMinDeltas.0-10").alias("damage_taken_diff_per_min_deltas_0_10"),
            F.col("participant.timeline.damageTakenDiffPerMinDeltas.10-20").alias("damage_taken_diff_per_min_deltas_10_20"),
            F.col("participant.timeline.damageTakenDiffPerMinDeltas.20-30").alias("damage_taken_diff_per_min_deltas_20_30"),
            F.col("participant.timeline.damageTakenDiffPerMinDeltas.30-end").alias("damage_taken_diff_per_min_deltas_30_end"),
            F.col("participant.timeline.role").alias("participant_timeline_role"),
            F.col("participant.timeline.lane").alias("participant_timeline_lane"),
            F.col("participantIdentity.participantId").alias("participant_identity_participant_id"),
            F.col("participantIdentity.player.platformId").alias("participant_identity_player_platform_id"),
            F.col("participantIdentity.player.accountId").alias("participant_identity_player_account_id"),
            F.col("participantIdentity.player.summonerId").alias("participant_identity_player_summoner_id"),
            "ts",
            "year",
            "month",
        ).withColumn("team_win", get_converted_str_to_boolean("team_win"))
    )

    # raw_data_formatted = raw_data_formatted.repartition("year", "month")

    (raw_data_formatted.write
     # .partitionBy("year", "month")
     .mode('overwrite')
     # .parquet(s3_destination_path))
     .json(s3_destination_path))


def main():
    """
    Orchestrates execution
    """
    spark = create_spark_session()
    process_matches_data(
        spark=spark,
        s3_raw_data_path=get_s3_path_for(aws_bucket=AWS_BUCKET, aws_data_keys=[AWS_RAW_DATA_KEY, AWS_MATCH_KEY]),
        s3_destination_path=get_s3_path_for(aws_bucket=AWS_BUCKET, aws_data_keys=[S3_TRANSFORMED_RAW_DATA_KEY, AWS_MATCH_KEY]),
    )


if __name__ == '__main__':
    main()
