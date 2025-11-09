using UnityEngine;
using UnityEngine.SceneManagement;
using DevelopersHub.ClashOfWhatecer;   // <-- нужно, чтобы видеть класс Player

public class EventManager : MonoBehaviour
{
    void Start()
    {

    }

    void Update()
    {

    }

    public void ActivateEvent(int eventId)
    {
        Debug.Log($"ActivateEvent: {eventId}");

        switch (eventId)
        {
            case 0: // WOOD (даЄм 0 золота, 100 эликсира)
                if (Player.instanse != null)
                {
                    Player.instanse.SendMapResources(0, 100, 0);
                    Debug.Log("ќтправлено на сервер: 0 золота, 100 эликсира, 0 гемов");
                }
                else
                {
                    Debug.LogWarning("Player instance is null Ч сервер не получит ресурсы!");
                }
                break;

            case 1: // IRON (даЄм 100 золота, 0 эликсира)
                if (Player.instanse != null)
                {
                    Player.instanse.SendMapResources(100, 0, 0);
                    Debug.Log("ќтправлено на сервер: 100 золота, 0 эликсира, 0 гемов");
                }
                else
                {
                    Debug.LogWarning("Player instance is null Ч сервер не получит ресурсы!");
                }
                // после отправки возвращаемс€ на базу
                SceneManager.LoadScene("Start");
                break;

            default:
                Debug.LogWarning("Ќеизвестный eventId");
                break;
        }
    }
}
