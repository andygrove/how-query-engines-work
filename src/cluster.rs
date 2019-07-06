use k8s_openapi::http;
use k8s_openapi::api::core::v1 as api;
use k8s_openapi;
use reqwest;

fn execute(request: http::Request<Vec<u8>>) -> Result<http::Response<Vec<u8>>, reqwest::Error> {

    println!("{:?}", request);

    let (method, path, body) = {
        let (parts, body) = request.into_parts();
        let mut url: http::uri::Parts = parts.uri.into();
        let path = url.path_and_query.take().expect("request doesn't have path and query");

        (parts.method, path, body)
    };

    let uri = format!("http://localhost:8080/{}", path);

    let mut x = reqwest::get(&uri)?;
    let body = x.text()?;

    let response = http::Response::builder()
        .status(http::StatusCode::OK)
        .body(body.as_bytes().to_vec())
        .unwrap();

    Ok(response)
}


fn list_pods() {

    let (request, response_body) = api::Pod::list_namespaced_pod("kube-system", Default::default()).expect("couldn't list pods");
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
            for pod in pod_list.items {
                println!("{:?}", pod.metadata.unwrap().name);
            }
        }

        _ => {
            println!("nope");
        }

//        // Some unexpected response
//        // (not HTTP 200, but still parsed successfully)
//        Ok(other) => return Err(format!(
//            "expected Ok but got {} {:?}",
//            status_code, other).into()),
//
//        // Need more response data.
//        // Read more bytes from the response into the `ResponseBody`
//        Err(k8s_openapi::ResponseError::NeedMoreData) => {},
//
//        // Some other error, like the response body being
//        // malformed JSON or invalid UTF-8.
//        Err(err) => return Err(format!(
//            "error: {} {:?}",
//            status_code, err).into()),
    }

}