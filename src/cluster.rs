use std::str;

use crate::error::BallistaError;
use k8s_openapi;
use k8s_openapi::api::batch::v1 as batch;
use k8s_openapi::api::core::v1 as api;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;
use k8s_openapi::http;
use reqwest;
use std::collections::BTreeMap;

fn execute(request: http::Request<Vec<u8>>) -> Result<http::Response<Vec<u8>>, BallistaError> {
    let (method, path, body) = {
        let (parts, body) = request.into_parts();
        let mut url: http::uri::Parts = parts.uri.into();
        let path = url
            .path_and_query
            .take()
            .expect("request doesn't have path and query");

        (parts.method, path, body)
    };

    let uri = format!("http://localhost:8080{}", path);

    let client = reqwest::Client::new();

    //    println!(
    //        "Request: {} {}{}",
    //        method,
    //        uri,
    //        str::from_utf8(&body).unwrap()
    //    );

    let mut x = match method {
        http::Method::GET => client.get(&uri).body(body).send()?,
        http::Method::POST => client.post(&uri).body(body).send()?,
        http::Method::DELETE => client.delete(&uri).body(body).send()?,
        _ => unimplemented!(),
    };

    let response_body = x.text()?;

    if x.status().is_success() {
        let response = http::Response::builder()
            .status(http::StatusCode::OK)
            .body(response_body.as_bytes().to_vec())
            .unwrap();

        Ok(response)
    } else {
        println!("Response: {}", response_body);

        Err(BallistaError::General("k8s api returned error".to_string()))
    }
}

pub fn create_ballista_executor(
    namespace: &str,
    name: &str,
    image_name: &str,
) -> Result<(), BallistaError> {
    create_pod(namespace, name, image_name)?;
    create_service(namespace, name)
}

pub fn create_service(namespace: &str, name: &str) -> Result<(), BallistaError> {
    let mut metadata: ObjectMeta = Default::default();
    metadata.name = Some(name.to_string());

    let mut spec: api::ServiceSpec = Default::default();
    spec.type_ = Some("ClusterIP".to_string());

    let mut labels = BTreeMap::new();
    labels.insert("ballista-name".to_string(), name.to_string());

    spec.selector = Some(labels);

    let mut port: api::ServicePort = Default::default();
    port.name = Some("grpc".to_string());
    port.port = 9090;
    port.target_port = Some(IntOrString::Int(9090));

    spec.ports = Some(vec![port]);

    let mut service: api::Service = Default::default();
    service.metadata = Some(metadata);
    service.spec = Some(spec);

    let (request, response_body) =
        api::Service::create_namespaced_service(namespace, &service, Default::default())
            .expect("couldn't create service");
    let response = execute(request).expect("couldn't create service");

    // Got a status code from executing the request.
    let status_code: http::StatusCode = response.status();

    let mut response_body = response_body(status_code);
    response_body.append_slice(&response.body());
    let response = response_body.parse();

    match response {
        // Successful response (HTTP 200 and parsed successfully)
        Ok(api::CreateNamespacedServiceResponse::Ok(service)) => {
            println!(
                "created service ok: {}",
                service.metadata.unwrap().name.unwrap()
            );
            Ok(())
        }

        // Some unexpected response
        // (not HTTP 200, but still parsed successfully)
        Ok(other) => return Err(format!("expected Ok but got {} {:?}", status_code, other).into()),

        // Need more response data.
        // Read more bytes from the response into the `ResponseBody`
        Err(k8s_openapi::ResponseError::NeedMoreData) => Err(BallistaError::General(
            "Need more response data".to_string(),
        )),

        // Some other error, like the response body being
        // malformed JSON or invalid UTF-8.
        Err(err) => return Err(format!("error: {} {:?}", status_code, err).into()),
    }
}

pub fn create_driver(namespace: &str, name: &str, image_name: &str) -> Result<(), BallistaError> {
    let mut pod_spec = create_pod_spec(name, image_name, false)?;
    pod_spec.restart_policy = Some("Never".to_string());

    let mut metadata: ObjectMeta = Default::default();
    metadata.name = Some(name.to_string());

    let mut container: api::Container = Default::default();
    container.name = name.to_string();
    container.image = Some(image_name.to_string());
    container.image_pull_policy = Some("Always".to_string()); //TODO make configurable

    let mut pod_template: api::PodTemplateSpec = Default::default();
    pod_template.spec = Some(pod_spec);

    let mut job_spec: batch::JobSpec = Default::default();
    job_spec.completions = Some(1);
    job_spec.template = pod_template;

    let pod = batch::Job {
        metadata: Some(metadata),
        spec: Some(job_spec),
        status: None,
    };

    let (request, response_body) =
        batch::Job::create_namespaced_job(namespace, &pod, Default::default())
            .expect("couldn't create job");
    let response = execute(request).expect("couldn't create job");

    // Got a status code from executing the request.
    let status_code: http::StatusCode = response.status();

    let mut response_body = response_body(status_code);
    response_body.append_slice(&response.body());
    let response = response_body.parse();

    match response {
        // Successful response (HTTP 200 and parsed successfully)
        Ok(batch::CreateNamespacedJobResponse::Ok(job)) => {
            println!("created job ok: {}", job.metadata.unwrap().name.unwrap());
            Ok(())
        }

        // Some unexpected response
        // (not HTTP 200, but still parsed successfully)
        Ok(other) => return Err(format!("expected Ok but got {} {:?}", status_code, other).into()),

        // Need more response data.
        // Read more bytes from the response into the `ResponseBody`
        Err(k8s_openapi::ResponseError::NeedMoreData) => Err(BallistaError::General(
            "Need more response data".to_string(),
        )),

        // Some other error, like the response body being
        // malformed JSON or invalid UTF-8.
        Err(err) => return Err(format!("error: {} {:?}", status_code, err).into()),
    }
}

pub fn create_pod(namespace: &str, name: &str, image_name: &str) -> Result<(), BallistaError> {
    let mut labels = BTreeMap::new();
    labels.insert("ballista-name".to_string(), name.to_string());

    let mut metadata: ObjectMeta = Default::default();
    metadata.name = Some(name.to_string());
    metadata.labels = Some(labels);

    let mut pod_spec = create_pod_spec(name, image_name, true)?;

    let pod = api::Pod {
        metadata: Some(metadata),
        spec: Some(pod_spec),
        status: None,
    };

    let (request, response_body) =
        api::Pod::create_namespaced_pod(namespace, &pod, Default::default())
            .expect("couldn't create pod");
    let response = execute(request).expect("couldn't create pod");

    // Got a status code from executing the request.
    let status_code: http::StatusCode = response.status();

    let mut response_body = response_body(status_code);
    response_body.append_slice(&response.body());
    let response = response_body.parse();

    match response {
        // Successful response (HTTP 200 and parsed successfully)
        Ok(api::CreateNamespacedPodResponse::Ok(pod)) => {
            println!("created pod ok: {}", pod.metadata.unwrap().name.unwrap());
            Ok(())
        }

        // Some unexpected response
        // (not HTTP 200, but still parsed successfully)
        Ok(other) => return Err(format!("expected Ok but got {} {:?}", status_code, other).into()),

        // Need more response data.
        // Read more bytes from the response into the `ResponseBody`
        Err(k8s_openapi::ResponseError::NeedMoreData) => Err(BallistaError::General(
            "Need more response data".to_string(),
        )),

        // Some other error, like the response body being
        // malformed JSON or invalid UTF-8.
        Err(err) => return Err(format!("error: {} {:?}", status_code, err).into()),
    }
}

pub fn create_pod_spec(
    name: &str,
    image_name: &str,
    executor: bool,
) -> Result<api::PodSpec, BallistaError> {
    let mut container: api::Container = Default::default();
    container.name = name.to_string();
    container.image = Some(image_name.to_string());
    container.image_pull_policy = Some("Always".to_string()); //TODO make configurable

    if executor {
        //TODO should not hard-code
        let mut volume_mount: api::VolumeMount = Default::default();
        volume_mount.name = "nyctaxi".to_string();
        volume_mount.read_only = Some(true);
        volume_mount.mount_path = "/mnt/ssd/nyc_taxis/csv".to_string();

        container.volume_mounts = Some(vec![volume_mount]);
    }

    let mut container_port: api::ContainerPort = Default::default();
    container_port.container_port = 9090;

    container.ports = Some(vec![container_port]);

    let mut pod_spec: api::PodSpec = Default::default();

    if executor {
        //TODO should not have hard-coded volume! need templating for this
        let mut volume: api::Volume = Default::default();
        volume.name = "nyctaxi".to_string();
        volume.host_path = Some(api::HostPathVolumeSource {
            path: "/mnt/ssd/nyc_taxis/csv".to_string(),
            type_: Some("".to_string()),
        });

        pod_spec.volumes = Some(vec![volume]);
    }

    pod_spec.containers = vec![container];

    Ok(pod_spec)
}

pub fn delete_pod(namespace: &str, pod_name: &str) -> Result<(), BallistaError> {
    let (request, response_body) =
        api::Pod::delete_namespaced_pod(pod_name, namespace, Default::default())
            .expect("couldn't delete pod");
    let response = execute(request).expect("couldn't delete pod");

    // Got a status code from executing the request.
    let status_code: http::StatusCode = response.status();

    let mut response_body = response_body(status_code);
    response_body.append_slice(&response.body());
    let response = response_body.parse();

    match response {
        // Successful response (HTTP 200 and parsed successfully)
        Ok(api::DeleteNamespacedPodResponse::OkStatus(_status)) => {
            //println!("deleted pod {} ok: status {:?}", pod_name, status);
            Ok(())
        }

        Ok(api::DeleteNamespacedPodResponse::OkValue(_value)) => {
            //println!("deleted pod {} ok: value {:?}", pod_name, value);
            Ok(())
        }

        Ok(api::DeleteNamespacedPodResponse::Accepted(_status)) => {
            //println!("deleted pod {} ok: accepted status {:?}", pod_name, status);
            Ok(())
        }

        // Some unexpected response
        // (not HTTP 200, but still parsed successfully)
        Ok(other) => return Err(format!("expected Ok but got {} {:?}", status_code, other).into()),

        // Need more response data.
        // Read more bytes from the response into the `ResponseBody`
        Err(k8s_openapi::ResponseError::NeedMoreData) => Err(BallistaError::General(
            "Need more response data".to_string(),
        )),

        // Some other error, like the response body being
        // malformed JSON or invalid UTF-8.
        Err(err) => return Err(format!("error: {} {:?}", status_code, err).into()),
    }
}

pub fn delete_service(namespace: &str, service_name: &str) -> Result<(), BallistaError> {
    let (request, response_body) =
        api::Service::delete_namespaced_service(service_name, namespace, Default::default())
            .expect("couldn't delete service");
    let response = execute(request).expect("couldn't delete service");

    // Got a status code from executing the request.
    let status_code: http::StatusCode = response.status();

    let mut response_body = response_body(status_code);
    response_body.append_slice(&response.body());
    let response = response_body.parse();

    match response {
        // Successful response (HTTP 200 and parsed successfully)
        Ok(api::DeleteNamespacedServiceResponse::OkStatus(_status)) => {
            //println!("deleted pod {} ok: status {:?}", pod_name, status);
            Ok(())
        }

        Ok(api::DeleteNamespacedServiceResponse::OkValue(_value)) => {
            //println!("deleted pod {} ok: value {:?}", pod_name, value);
            Ok(())
        }

        Ok(api::DeleteNamespacedServiceResponse::Accepted(_status)) => {
            //println!("deleted pod {} ok: accepted status {:?}", pod_name, status);
            Ok(())
        }

        // Some unexpected response
        // (not HTTP 200, but still parsed successfully)
        Ok(other) => return Err(format!("expected Ok but got {} {:?}", status_code, other).into()),

        // Need more response data.
        // Read more bytes from the response into the `ResponseBody`
        Err(k8s_openapi::ResponseError::NeedMoreData) => Err(BallistaError::General(
            "Need more response data".to_string(),
        )),

        // Some other error, like the response body being
        // malformed JSON or invalid UTF-8.
        Err(err) => return Err(format!("error: {} {:?}", status_code, err).into()),
    }
}

pub fn list_pods(namespace: &str) -> Result<Vec<String>, BallistaError> {
    let (request, response_body) =
        api::Pod::list_namespaced_pod(namespace, Default::default()).expect("couldn't list pods");
    let response = execute(request).expect("couldn't list pods");

    // Got a status code from executing the request.
    let status_code: http::StatusCode = response.status();

    // Construct the `ResponseBody<ListNamespacedPodResponse>` using the
    // constructor returned by the API function.
    let mut response_body = response_body(status_code);

    response_body.append_slice(&response.body());

    let response = response_body.parse();
    match response {
        // Successful response (HTTP 200 and parsed successfully)
        Ok(api::ListNamespacedPodResponse::Ok(pod_list)) => {
            let mut ret = vec![];
            for pod in pod_list.items {
                ret.push(pod.metadata.unwrap().name.unwrap());
            }
            Ok(ret)
        }

        // Some unexpected response
        // (not HTTP 200, but still parsed successfully)
        Ok(other) => return Err(format!("expected Ok but got {} {:?}", status_code, other).into()),

        // Need more response data.
        // Read more bytes from the response into the `ResponseBody`
        Err(k8s_openapi::ResponseError::NeedMoreData) => Err(BallistaError::General(
            "Need more response data".to_string(),
        )),

        // Some other error, like the response body being
        // malformed JSON or invalid UTF-8.
        Err(err) => return Err(format!("error: {} {:?}", status_code, err).into()),
    }
}
