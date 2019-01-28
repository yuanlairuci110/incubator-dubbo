/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.dubbo.registry.zookeeper;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.common.utils.ConcurrentHashSet;
import org.apache.dubbo.common.utils.UrlUtils;
import org.apache.dubbo.registry.NotifyListener;
import org.apache.dubbo.registry.support.FailbackRegistry;
import org.apache.dubbo.remoting.zookeeper.ChildListener;
import org.apache.dubbo.remoting.zookeeper.StateListener;
import org.apache.dubbo.remoting.zookeeper.ZookeeperClient;
import org.apache.dubbo.remoting.zookeeper.ZookeeperTransporter;
import org.apache.dubbo.rpc.RpcException;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * ZookeeperRegistry
 */
public class ZookeeperRegistry extends FailbackRegistry {

    private final static Logger logger = LoggerFactory.getLogger(ZookeeperRegistry.class);

    private final static int DEFAULT_ZOOKEEPER_PORT = 2181;

    private final static String DEFAULT_ROOT = "dubbo";

    private final String root;

    private final Set<String> anyServices = new ConcurrentHashSet<>();

    private final ConcurrentMap<URL, ConcurrentMap<NotifyListener, ChildListener>> zkListeners = new ConcurrentHashMap<URL, ConcurrentMap<NotifyListener, ChildListener>>();

    private final ZookeeperClient zkClient;

    /**
     * AbstractRegistry ：主要用来维护缓存文件。
     *      1、设置属性registryUrl=url：zookeeper://10.211.55.5:2181/com.alibaba.dubbo.registry.RegistryService?application=demo-provider&client=curator&dubbo=2.0.0&interface=com.alibaba.dubbo.registry.RegistryService&pid=4685&timestamp=1507286468150
     *      2、创建文件/Users/lc/.dubbo/dubbo-registry-10.211.55.5.cache的文件夹/Users/jigangzhao/.dubbo
     *      3、设置属性file：/Users/lc/.dubbo/dubbo-registry-10.211.55.5.cache文件，该文件存储信息将是这样的：
     *      com.alibaba.dubbo.demo.DemoService=empty\://10.10.10.10\:20880/com.alibaba.dubbo.demo.DemoService?anyhost\=true&application\=demo-provider&category\=configurators&check\=false&dubbo\=2.0.0&generic\=false&interface\=com.alibaba.dubbo.demo.DemoService&methods\=sayHello&pid\=5259&side\=provider&timestamp\=1507294508053
     *      4、如果file存在，将file中的内容写入properties属性；既然有读file，那么是什么时候写入file的呢？AbstractRegistry创建了一个含有一个名字为DubboSaveRegistryCache的后台线程的FixedThreadPool，只在在notify(URL url, NotifyListener listener, List<URL> urls)方法中会被调用，我们此处由于ConcurrentMap<URL, Set<NotifyListener>> subscribed为空，所以AbstractRegistry(URL url)中的notify(url.getBackupUrls())不会执行，此处也不会创建文件。
     *      5、最后是notify(url.getBackupUrls())（这里后续会写）
     * FailbackRegistry ：主要用来做失败重试操作（包括：注册失败／反注册失败／订阅失败／反订阅失败／通知失败的重试）；也提供了供ZookeeperRegistry使用的zk重连后的恢复工作的方法。
     *      启动了一个含有一个名为DubboRegistryFailedRetryTimer的后台线程的ScheduledThreadPool，线程创建5s后开始第一次执行retry()，之后每隔5s执行一次
     * ZookeeperRegistry ：创建zk客户端，启动会话；并且调用FailbackRegistry实现zk重连后的恢复工作。
     *      1、属性设置root=/dubbo
     *      2、创建zk客户端，启动会话
     *          org.apache.dubbo.remoting.zookeeper.support.AbstractZookeeperTransporter#connect(org.apache.dubbo.common.URL)创建CuratorZookeeperClient
     *          ZookeeperRegistry实例属性：
     *              ZookeeperClient zkClient = CuratorZookeeperClient实例
     *                  CuratorFramework client：CuratorFrameworkImpl实例
     *                  String url：zookeeper://10.211.55.5:2181/com.alibaba.dubbo.registry.RegistryService?application=demo-provider&client=curator&dubbo=2.0.0&interface=com.alibaba.dubbo.registry.RegistryService&pid=4685&timestamp=1507286468150
     *                  Set<StateListener> stateListeners：{ 监听了重连成功事件的执行recover()的StateListener }
     *              String root="/dubbo"
     *              URL registryUrl = zookeeper://10.211.55.5:2181/com.alibaba.dubbo.registry.RegistryService?application=demo-provider&client=curator&dubbo=2.0.0&interface=com.alibaba.dubbo.registry.RegistryService&pid=4685&timestamp=1507286468150
     *              Set<URL> registered：0//已经注册的url集合，此处为空
     *              ConcurrentMap<URL, Set<NotifyListener>> subscribed：0//已经订阅的<URL, Set<NotifyListener>>
     *              ConcurrentMap<URL, Map<String, List<URL>>> notified：0//已经通知的<URL, Map<String, List<URL>>>
     *              Set<URL> failedRegistered：0//注册失败的url
     *              Set<URL> failedUnregistered：0//反注册失败的url
     *              ConcurrentMap<URL, Set<NotifyListener>> failedSubscribed：0//订阅失败的url
     *              ConcurrentMap<URL, Set<NotifyListener>> failedUnsubscribed：0//反订阅失败的url
     *              ConcurrentMap<URL, Map<NotifyListener, List<URL>>> failedNotified：0//通知失败的url
     *              ConcurrentMap<URL, ConcurrentMap<NotifyListener, ChildListener>> zkListeners：0
     *      ZookeeperRegistry会存储在ZookeeperRegistry的父类的static属性Map<String, Registry> REGISTRIES中
     *      Map<String, Registry> REGISTRIES：{ "zookeeper://10.211.55.5:2181/com.alibaba.dubbo.registry.RegistryService" : ZookeeperRegistry实例 }
     *      3、创建了一个StateListener监听器，监听重新连接成功事件，重新连接成功后，之前已经完成注册和订阅的url要重新进行注册和订阅（因为临时节点可能已经跪了）。
     *
     */
    public ZookeeperRegistry(URL url, ZookeeperTransporter zookeeperTransporter) {
        super(url);
        if (url.isAnyHost()) {
            throw new IllegalStateException("registry address == null");
        }
        String group = url.getParameter(Constants.GROUP_KEY, DEFAULT_ROOT);
        if (!group.startsWith(Constants.PATH_SEPARATOR)) {
            group = Constants.PATH_SEPARATOR + group;
        }
        this.root = group;
        // 创建zk客户端，启动会话
        zkClient = zookeeperTransporter.connect(url);
        // 监听重新连接成功事件，重新连接成功后，之前已经完成注册和订阅的url要重新进行注册和订阅（因为临时节点可能已经跪了）
        zkClient.addStateListener(state -> {
            if (state == StateListener.RECONNECTED) {
                try {
                    recover();
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                }
            }
        });
    }

    static String appendDefaultPort(String address) {
        if (address != null && address.length() > 0) {
            int i = address.indexOf(':');
            if (i < 0) {
                return address + ":" + DEFAULT_ZOOKEEPER_PORT;
            } else if (Integer.parseInt(address.substring(i + 1)) == 0) {
                return address.substring(0, i + 1) + DEFAULT_ZOOKEEPER_PORT;
            }
        }
        return address;
    }

    @Override
    public boolean isAvailable() {
        return zkClient.isConnected();
    }

    @Override
    public void destroy() {
        super.destroy();
        try {
            zkClient.close();
        } catch (Exception e) {
            logger.warn("Failed to close zookeeper client " + getUrl() + ", cause: " + e.getMessage(), e);
        }
    }

    @Override
    public void doRegister(URL url) {
        try {
            zkClient.create(toUrlPath(url), url.getParameter(Constants.DYNAMIC_KEY, true));
        } catch (Throwable e) {
            throw new RpcException("Failed to register " + url + " to zookeeper " + getUrl() + ", cause: " + e.getMessage(), e);
        }
    }

    @Override
    public void doUnregister(URL url) {
        try {
            zkClient.delete(toUrlPath(url));
        } catch (Throwable e) {
            throw new RpcException("Failed to unregister " + url + " to zookeeper " + getUrl() + ", cause: " + e.getMessage(), e);
        }
    }

    // 当前的provider订阅了/dubbo/com.alibaba.dubbo.demo.DemoService/configurators，当其下的子节点发生变化时，如果其下的子节点的url或者当前的providerUrl发生了变化，需要重新暴露。
    @Override
    public void doSubscribe(final URL url, final NotifyListener listener) {
        try {
            if (Constants.ANY_VALUE.equals(url.getServiceInterface())) {
                String root = toRootPath();
                ConcurrentMap<NotifyListener, ChildListener> listeners = zkListeners.get(url);
                if (listeners == null) {
                    zkListeners.putIfAbsent(url, new ConcurrentHashMap<>());
                    listeners = zkListeners.get(url);
                }
                ChildListener zkListener = listeners.get(listener);
                if (zkListener == null) {
                    listeners.putIfAbsent(listener, (parentPath, currentChilds) -> {
                        for (String child : currentChilds) {
                            child = URL.decode(child);
                            if (!anyServices.contains(child)) {
                                anyServices.add(child);
                                subscribe(url.setPath(child).addParameters(Constants.INTERFACE_KEY, child,
                                        Constants.CHECK_KEY, String.valueOf(false)), listener);
                            }
                        }
                    });
                    zkListener = listeners.get(listener);
                }
                zkClient.create(root, false);
                List<String> services = zkClient.addChildListener(root, zkListener);
                if (CollectionUtils.isNotEmpty(services)) {
                    for (String service : services) {
                        service = URL.decode(service);
                        anyServices.add(service);
                        subscribe(url.setPath(service).addParameters(Constants.INTERFACE_KEY, service,
                                Constants.CHECK_KEY, String.valueOf(false)), listener);
                    }
                }
            } else {
                /**
                 * ConcurrentMap<URL, ConcurrentMap<NotifyListener, ChildListener>> zkListeners
                 *
                 * 1、根据url获取ConcurrentMap<NotifyListener, ChildListener>，没有就创建
                 * 2、根据listener从ConcurrentMap<NotifyListener, ChildListener>获取ChildListener，没有就创建（创建的ChildListener用来监听子节点的变化）
                 * 3、创建path持久化节点
                 * 4、创建path子节点监听器
                 */
                List<URL> urls = new ArrayList<>();
                // 首先获取categorypath：实际上就是获取/dubbo/{servicename}/{url中的category参数，默认是providers，这里是final URL overrideSubscribeUrl = getSubscribedOverrideUrl(registedProviderUrl);这句代码中添加到overrideSubscribeUrl上的category=configurators}
                for (String path : toCategoriesPath(url)) {
                    ConcurrentMap<NotifyListener, ChildListener> listeners = zkListeners.get(url);
                    if (listeners == null) {
                        zkListeners.putIfAbsent(url, new ConcurrentHashMap<>());
                        listeners = zkListeners.get(url);
                    }
                    ChildListener zkListener = listeners.get(listener);
                    if (zkListener == null) {
                        listeners.putIfAbsent(listener, (parentPath, currentChilds) -> ZookeeperRegistry.this.notify(url, listener, toUrlsWithEmpty(url, parentPath, currentChilds)));
                        zkListener = listeners.get(listener);
                    }
                    // 创建持久化节点/dubbo/com.alibaba.dubbo.demo.DemoService/configurators
                    zkClient.create(path, false);
                    // 然后使用AbstractZookeeperClient<TargetChildListener>的addChildListener(String path, final ChildListener listener)方法为path下的子节点添加上边创建出来的内部类ChildListener实例
                    List<String> children = zkClient.addChildListener(path, zkListener);
                    if (children != null) {
                        urls.addAll(toUrlsWithEmpty(url, path, children));
                    }
                }
                notify(url, listener, urls);
            }
        } catch (Throwable e) {
            throw new RpcException("Failed to subscribe " + url + " to zookeeper " + getUrl() + ", cause: " + e.getMessage(), e);
        }
    }

    @Override
    public void doUnsubscribe(URL url, NotifyListener listener) {
        ConcurrentMap<NotifyListener, ChildListener> listeners = zkListeners.get(url);
        if (listeners != null) {
            ChildListener zkListener = listeners.get(listener);
            if (zkListener != null) {
                if (Constants.ANY_VALUE.equals(url.getServiceInterface())) {
                    String root = toRootPath();
                    zkClient.removeChildListener(root, zkListener);
                } else {
                    for (String path : toCategoriesPath(url)) {
                        zkClient.removeChildListener(path, zkListener);
                    }
                }
            }
        }
    }

    @Override
    public List<URL> lookup(URL url) {
        if (url == null) {
            throw new IllegalArgumentException("lookup url == null");
        }
        try {
            List<String> providers = new ArrayList<>();
            for (String path : toCategoriesPath(url)) {
                List<String> children = zkClient.getChildren(path);
                if (children != null) {
                    providers.addAll(children);
                }
            }
            return toUrlsWithoutEmpty(url, providers);
        } catch (Throwable e) {
            throw new RpcException("Failed to lookup " + url + " from zookeeper " + getUrl() + ", cause: " + e.getMessage(), e);
        }
    }

    private String toRootDir() {
        if (root.equals(Constants.PATH_SEPARATOR)) {
            return root;
        }
        return root + Constants.PATH_SEPARATOR;
    }

    private String toRootPath() {
        return root;
    }

    private String toServicePath(URL url) {
        String name = url.getServiceInterface();
        if (Constants.ANY_VALUE.equals(name)) {
            return toRootPath();
        }
        return toRootDir() + URL.encode(name);
    }

    private String[] toCategoriesPath(URL url) {
        String[] categories;
        if (Constants.ANY_VALUE.equals(url.getParameter(Constants.CATEGORY_KEY))) {
            categories = new String[]{Constants.PROVIDERS_CATEGORY, Constants.CONSUMERS_CATEGORY,
                    Constants.ROUTERS_CATEGORY, Constants.CONFIGURATORS_CATEGORY};
        } else {
            categories = url.getParameter(Constants.CATEGORY_KEY, new String[]{Constants.DEFAULT_CATEGORY});
        }
        String[] paths = new String[categories.length];
        for (int i = 0; i < categories.length; i++) {
            paths[i] = toServicePath(url) + Constants.PATH_SEPARATOR + categories[i];
        }
        return paths;
    }

    private String toCategoryPath(URL url) {
        return toServicePath(url) + Constants.PATH_SEPARATOR + url.getParameter(Constants.CATEGORY_KEY, Constants.DEFAULT_CATEGORY);
    }

    private String toUrlPath(URL url) {
        return toCategoryPath(url) + Constants.PATH_SEPARATOR + URL.encode(url.toFullString());
    }

    private List<URL> toUrlsWithoutEmpty(URL consumer, List<String> providers) {
        List<URL> urls = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(providers)) {
            for (String provider : providers) {
                provider = URL.decode(provider);
                if (provider.contains(Constants.PROTOCOL_SEPARATOR)) {
                    URL url = URL.valueOf(provider);
                    if (UrlUtils.isMatch(consumer, url)) {
                        urls.add(url);
                    }
                }
            }
        }
        return urls;
    }

    private List<URL> toUrlsWithEmpty(URL consumer, String path, List<String> providers) {
        List<URL> urls = toUrlsWithoutEmpty(consumer, providers);
        if (urls == null || urls.isEmpty()) {
            int i = path.lastIndexOf(Constants.PATH_SEPARATOR);
            String category = i < 0 ? path : path.substring(i + 1);
            URL empty = consumer.setProtocol(Constants.EMPTY_PROTOCOL).addParameter(Constants.CATEGORY_KEY, category);
            urls.add(empty);
        }
        return urls;
    }

}
