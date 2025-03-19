<template>
  <div class="title">
    <h1>Statistics Content</h1>
  </div>
  <div class="content-main">
    <NoAnswerPercentage/>
    <AverageMaxAnswer/>
    <AnswerDistribution/>
    <QuesionWithAcceptedAnswer/>
    <ResolutionTimeDistribution/>
    <BetterAnswerThanAccepted/>
    <FrequentTag/>
    <TagUpvotes/>
    <TagViews/>
    <UserParticipation/>
    <UserAnswerParticipation/>
    <UserCommentParticipation/>
    <ActiveUser/>
    <ApiCount/>
  </div>
</template>

<script>
import StatisticsContainer from "@/components/StatisticsContainer.vue";
import NoAnswerPercentage from "@/components/data-containers/NoAnswerPercentage.vue";
import AverageMaxAnswer from "@/components/data-containers/AverageMaxAnswer.vue";
import AnswerDistribution from "@/components/data-containers/AnswerDistribution.vue";
import QuesionWithAcceptedAnswer from "@/components/data-containers/QuestionWithAcceptedAnswer.vue";
import ResolutionTimeDistribution from "@/components/data-containers/ResolutionTimeDistribution.vue";
import BetterAnswerThanAccepted from "@/components/data-containers/BetterAnswerThanAccepted.vue";
import FrequentTag from "@/components/data-containers/FrequentTag.vue";
import TagUpvotes from "@/components/data-containers/TagUpvotes.vue";
import TagViews from "@/components/data-containers/TagViews.vue";
import UserParticipation from "@/components/data-containers/UserParticipation.vue";
import UserAnswerParticipation from "@/components/data-containers/UserAnswerParticipation.vue";
import UserCommentParticipation from "@/components/data-containers/UserCommentParticipation.vue";
import ActiveUser from "@/components/data-containers/ActiveUser.vue";
import ApiCount from "@/components/data-containers/ApiCount.vue";

export default {
  name: "ContentComponent",
  components: {
    ApiCount,
    ActiveUser,
    UserCommentParticipation,
    UserAnswerParticipation,
    UserParticipation,
    TagViews,
    TagUpvotes,
    FrequentTag,
    BetterAnswerThanAccepted,
    ResolutionTimeDistribution,
    QuesionWithAcceptedAnswer,
    AnswerDistribution, AverageMaxAnswer, NoAnswerPercentage, StatisticsContainer}
}
</script>

<style scoped>
  .title {
    margin-top: 50px;
    caret-color: transparent;
    display: flex;
    justify-content: center;
  }

  .title > h1 {
    font-size: 2.6rem;
    font-weight: 500;
    color: var(--color-heading);
  }

  .content-main {
    margin: 100px 100px 100px 100px;
    display: flex;
    justify-content: space-between;
    flex-flow: wrap;
    flex-direction: row;
    align-items: flex-start;
    align-content: space-between;
    gap: 40px;
  }


</style>