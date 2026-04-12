import Foundation

struct CRMService {
  private let session: URLSession

  init(session: URLSession? = nil) {
    if let session {
      self.session = session
      return
    }

    let configuration = URLSessionConfiguration.ephemeral
    configuration.httpCookieStorage = HTTPCookieStorage.shared
    configuration.httpShouldSetCookies = true
    configuration.requestCachePolicy = .reloadIgnoringLocalCacheData
    configuration.urlCache = nil
    configuration.waitsForConnectivity = true
    configuration.timeoutIntervalForRequest = 30
    configuration.timeoutIntervalForResource = 60
    self.session = URLSession(configuration: configuration)
  }

  func me() async throws -> CRMMeResponse {
    try await request(path: "/api/metalworks-crm/me")
  }

  func login(email: String, password: String) async throws -> CRMLoginResponse {
    try await request(
      path: "/api/metalworks-crm/login",
      method: "POST",
      body: CRMLoginRequest(email: email, password: password)
    )
  }

  func logout() async throws {
    let _: EmptyResponse = try await request(path: "/api/metalworks-crm/logout", method: "POST")
  }

  func dashboard(status: String = "", search: String = "", projectType: String = "") async throws -> CRMDashboardResponse {
    var queryItems: [URLQueryItem] = []

    if !status.isEmpty {
      queryItems.append(URLQueryItem(name: "status", value: status))
    }

    if !search.isEmpty {
      queryItems.append(URLQueryItem(name: "search", value: search))
    }

    if !projectType.isEmpty {
      queryItems.append(URLQueryItem(name: "projectType", value: projectType))
    }

    return try await request(path: "/api/metalworks-crm/dashboard", queryItems: queryItems)
  }

  func applicants() async throws -> CRMApplicantsResponse {
    try await request(path: "/api/metalworks-crm/applicants")
  }

  func leadDetail(id: String) async throws -> CRMLeadDetailResponse {
    try await request(path: "/api/metalworks-crm/leads/\(id)")
  }

  func applicantDetail(id: String) async throws -> CRMApplicantDetailResponse {
    try await request(path: "/api/metalworks-crm/applicants/\(id)")
  }

  func updateApplicant(id: String, payload: CRMApplicantUpdatePayload) async throws -> CRMApplicantDetailResponse {
    try await request(
      path: "/api/metalworks-crm/applicants/\(id)",
      method: "PATCH",
      body: payload
    )
  }

  func updateLead(id: String, payload: CRMLeadUpdatePayload) async throws -> CRMLeadDetailResponse {
    try await request(
      path: "/api/metalworks-crm/leads/\(id)",
      method: "PATCH",
      body: payload
    )
  }

  func registerPushDevice(payload: CRMPushRegistrationPayload) async throws -> CRMPushRegistrationResponse {
    try await request(
      path: "/api/metalworks-crm/push/register",
      method: "POST",
      body: payload
    )
  }

  func sendPushTest() async throws -> CRMPushTestResponse {
    try await request(path: "/api/metalworks-crm/push/test", method: "POST")
  }

  func assetURL(relativePath: String) -> URL? {
    guard !relativePath.isEmpty else {
      return nil
    }

    return AppConfig.apiBaseURL.appendingAPIPath(relativePath)
  }

  private func request<Response: Decodable>(
    path: String,
    method: String = "GET",
    queryItems: [URLQueryItem] = []
  ) async throws -> Response {
    try await request(path: path, method: method, queryItems: queryItems, body: Optional<String>.none)
  }

  private func request<Response: Decodable, Body: Encodable>(
    path: String,
    method: String = "GET",
    queryItems: [URLQueryItem] = [],
    body: Body?
  ) async throws -> Response {
    var components = URLComponents(url: AppConfig.apiBaseURL.appendingAPIPath(path), resolvingAgainstBaseURL: false)
    components?.queryItems = queryItems.isEmpty ? nil : queryItems

    guard let url = components?.url else {
      throw CRMServiceError.invalidResponse
    }

    var request = URLRequest(url: url)
    request.httpMethod = method
    request.httpShouldHandleCookies = true
    request.cachePolicy = .reloadIgnoringLocalCacheData
    request.setValue("no-cache", forHTTPHeaderField: "Cache-Control")
    request.setValue("no-cache", forHTTPHeaderField: "Pragma")

    if let body {
      request.setValue("application/json", forHTTPHeaderField: "Content-Type")
      request.httpBody = try JSONEncoder().encode(body)
    }

    let (data, response) = try await session.data(for: request)
    guard let httpResponse = response as? HTTPURLResponse else {
      throw CRMServiceError.invalidResponse
    }

    let decoder = JSONDecoder()

    if (200 ..< 300).contains(httpResponse.statusCode) {
      return try decoder.decode(Response.self, from: data)
    }

    let message = (try? decoder.decode(CRMErrorEnvelope.self, from: data).error)
      ?? "The CRM request could not be completed."

    if httpResponse.statusCode == 401 || httpResponse.statusCode == 403 {
      throw CRMServiceError.unauthorized(message)
    }

    throw CRMServiceError.server(message)
  }
}

private struct EmptyResponse: Decodable {}

private struct CRMLoginRequest: Encodable {
  let email: String
  let password: String
}

private struct CRMErrorEnvelope: Decodable {
  let error: String
}

enum CRMServiceError: LocalizedError {
  case invalidResponse
  case unauthorized(String)
  case server(String)

  var errorDescription: String? {
    switch self {
    case .invalidResponse:
      return "The CRM returned an invalid response."
    case .unauthorized(let message):
      return message
    case .server(let message):
      return message
    }
  }
}
