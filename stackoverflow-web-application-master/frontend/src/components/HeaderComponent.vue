
<style scoped>

header {
  background-color: var(--color-background-soft);
  padding: 10px 20px 10px 20px;
  display: flex;
  justify-content: start;
  align-items: center;
  flex-direction: row;
  caret-color: transparent;
  user-select: none;
  top: 0;
  position: fixed;
  min-width: 100%;
  width: max-content;
  z-index: 2002;
}

header > .items:nth-child(3) {
  flex-grow: 1;
}

.items {
  display: flex;
  justify-content: flex-end;
  align-items: center;
}

.logo-header {
  display: flex;
  margin-right: 40px;
}

.logo-header > a > img {
  width: 62px;
  height: 62px;
}

.logo-text-block {
  display: flex;
  align-items: baseline;
}

.logo-text {
  font-size: 2.6rem;
  font-weight: 500;
  color: var(--color-heading);
}

.logo-text-amp {
  font-size: 1.6rem;
  font-weight: 400;
  color: var(--color-heading-sub);
  margin-left: 10px;
  font-style: italic;
}

.update-time-box {
  height: 100%;
  width: max-content;
  border: solid;
  border-width: 2px;
  border-color: var(--color-border);
  background-color: transparent;
  padding: 10px 10px 10px 10px;
  text-align: center;
  border-radius: 10px;
  transition: 0.3s;
  display: flex;
  gap: 5px;
  align-items: center;
}

.link-icon {
  padding: 7px 7px 7px 7px;
  border-radius: 10px;
  transition: 0.2s;
}

.link-icon:hover {
  background-color: rgb(255, 255, 255, 0.2);
}


.update-time-box:hover {
  background-color: var(--color-background-soft-hover);
  color: var(--color-text-hover);
  border-color: var(--color-border-hover);
}

.update-time-box:hover .update-time-box-item {
  background-color: var(--color-border-hover);
  color: var(--color-background-soft);
  transition: 0.7s;
}

.update-time-box .update-time-box-item {
  padding: 1px 5px 1px 5px;
  background-color: var(--color-border);
  color: lightcyan;
  font-weight: BOLD;
  border-radius: 5px;
}

</style>

<template>
  <header ref="header">
    <div class="logo-header">
      <a href="https://stackoverflow.com/">
        <img alt="sof logo" src="../assets/logo.svg"/>
      </a>
      <div class="logo-text-block">
        <div class="logo-text">Stack Overflow</div>
        <div class="logo-text-amp">web application</div>
      </div>
    </div>
    <div class="items">
      <div class="update-time-box">
        <div class="update-time-box-item">
          <h3>Update Time</h3>
        </div>
        <h3>{{ last_update_time_string }}</h3>
      </div>
    </div>
    <div class="items">
      <div class="link-icon">
        <a href="https://github.com/XiaoLeGG/stackoverflow-web-application">
          <img alt="github logo" src="../assets/github-mark-white.svg" width="31" height="31"/>
        </a>
      </div>
    </div>
  </header>
</template>

<script>
import {ref} from "vue";
import axios from "axios";
import {ElMessage} from "element-plus";

export default {
  name: "HeaderComponent",
  data() {
    return {
      height: 40,
      last_update_time_string: ref("Unknown"),
      last_update_time: ref()
    }
  },
  mounted() {
    this.height = this.$refs.header.clientHeight;

    axios.get('/api/last_update/time')
      .then(response => {
        this.last_update_time = new Date(response.data);
        this.last_update_time_string = this.last_update_time.toLocaleString();
      })
      .catch(error => {
        ElMessage(error);
      })
  }
}
</script>
