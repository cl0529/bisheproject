export function baiduMap() {
  const baiduKey = "tW8SbdAVdwbLCkxckPICYxNtXRbSAOXk"
  const BMap_URL = "https://api.map.baidu.com/api?v=2.0&ak="+ baiduKey +"&s=1&callback=onBMapCallback";
  return new Promise((resolve, reject) => {
    // 如果已加载直接返回
    if(typeof BMap !== "undefined") {
      resolve(BMap);
      return true;
    }
    // 百度地图异步加载回调处理
    window.onBMapCallback = function () {
      console.log("百度地图脚本初始化成功...");
      resolve(BMap);
    };

    // 插入script脚本
    let script = document.createElement("script");
    script.setAttribute("type", "text/javascript");
    script.setAttribute("src", BMap_URL);
    script.onerror = reject;
    document.body.appendChild(script);
  });

}