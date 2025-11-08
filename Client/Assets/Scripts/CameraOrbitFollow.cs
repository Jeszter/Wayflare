using UnityEngine;

public class CameraOrbitFollow : MonoBehaviour
{
    [Header("Кого преследуем")]
    public Transform target;

    [Header("Дистанция")]
    public float distance = 16f;       
    public float minDistance = 20f;       
    public float maxDistance = 150f;    
    public float zoomSpeed = 3f;        

    [Header("Орбита")]
    public float yaw = 0f;
    public float pitch = 20f;
    public float minPitch = 5f;
    public float maxPitch = 60f;
    public float rotateSpeed = 120f;

    [Header("Плавность")]
    public float followSmooth = 10f;

    private Vector2 _lastPanPos;
    private bool _isDragging = false;

    void LateUpdate()
    {
        if (!target) return;

        HandleInput();

        Quaternion rot = Quaternion.Euler(pitch, yaw, 0f);
        Vector3 desiredPos = target.position - rot * Vector3.forward * distance;

        transform.position = Vector3.Lerp(transform.position, desiredPos, followSmooth * Time.deltaTime);
        transform.LookAt(target.position);
    }

    void HandleInput()
    {
#if UNITY_EDITOR || UNITY_STANDALONE
        if (Input.GetMouseButtonDown(0))
        {
            _isDragging = true;
            _lastPanPos = Input.mousePosition;
        }
        else if (Input.GetMouseButtonUp(0))
        {
            _isDragging = false;
        }

        if (_isDragging)
        {
            Vector2 delta = (Vector2)Input.mousePosition - _lastPanPos;
            _lastPanPos = Input.mousePosition;

            yaw += delta.x * rotateSpeed * Time.deltaTime * 0.2f;
            pitch -= delta.y * rotateSpeed * Time.deltaTime * 0.2f;
            pitch = Mathf.Clamp(pitch, minPitch, maxPitch);
        }

        // зум колесиком
        float scroll = Input.GetAxis("Mouse ScrollWheel");
        if (Mathf.Approximately(scroll, 0f))
            scroll = Input.mouseScrollDelta.y;

        if (Mathf.Abs(scroll) > 0.001f)
        {
            distance -= scroll * zoomSpeed * 2f;
            distance = Mathf.Clamp(distance, minDistance, maxDistance);
        }

#elif UNITY_ANDROID || UNITY_IOS
        if (Input.touchCount == 1)
        {

            Touch t = Input.GetTouch(0);
            if (t.phase == TouchPhase.Moved)
            {
                Vector2 delta = t.deltaPosition;

                yaw += delta.x * rotateSpeed * Time.deltaTime * 0.1f;
                pitch -= delta.y * rotateSpeed * Time.deltaTime * 0.1f;
                pitch = Mathf.Clamp(pitch, minPitch, maxPitch);
            }
        }
        else if (Input.touchCount == 2)
        {
            Touch t0 = Input.GetTouch(0);
            Touch t1 = Input.GetTouch(1);

            Vector2 t0Prev = t0.position - t0.deltaPosition;
            Vector2 t1Prev = t1.position - t1.deltaPosition;

            float prevMag = (t0Prev - t1Prev).magnitude;
            float currentMag = (t0.position - t1.position).magnitude;
            float diff = currentMag - prevMag;

            distance -= diff * 0.01f * zoomSpeed;
            distance = Mathf.Clamp(distance, minDistance, maxDistance);
        }
#endif
    }
}
