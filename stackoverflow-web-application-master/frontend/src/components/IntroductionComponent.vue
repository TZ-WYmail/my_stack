<template>
  <div class="intro-block">
    <div class="introduction-text">
      <div class="introduction-title">
        <h1>Introduction</h1>
      </div>
      <div class="introduction-content">
        Stack Overflow Web Application is a web application that collect and analyze statistics of Stack Overflow such as the most popular tags, the most popular questions, the most popular users, etc.
      </div>
    </div>
    <img src="../assets/bg/sof.png" style="width: 40%; height: auto; user-select: none; border-radius: 20px; -webkit-filter: drop-shadow(10px 10px 10px rgba(0,0,0,.2));"/>
  </div>
</template>

<script>
export default {
  name: "IntroductionComponent"
}
</script>

<style scoped>

.intro-block {
  padding-top: 100px;
  display: flex;
  flex-direction: row;
  justify-content: center;
  caret-color: transparent;
  padding-bottom: 70px;
  border-bottom: solid 1px var(--color-background-soft);
}

.introduction-text {
  display: flex;
  flex-direction: column;
  align-items: start;
  justify-content: center;
  width: 40%;
  padding: 20px 40px 20px 40px;
  margin-right: 40px;
  border-radius: 50px;


}

.introduction-title > h1 {
  margin-bottom: 10px;
  font-size: 2.6rem;
  font-weight: 500;
  color: var(--color-heading);
}

.introduction-content {
  text-align: left;
  font-size: 1.2rem;
  color: var(--color-text);
  font-weight: 400;
  line-height: 1.5;
  margin-bottom: 10px;
}

</style>